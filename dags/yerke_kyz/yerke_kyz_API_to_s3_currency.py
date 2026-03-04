from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.telegram.operators.telegram import TelegramOperator

import io 
import pandas as pd
import requests
import pendulum

#script for the task "upload"
def api_to_s3(**context):
    ####  FROM API
    url = 'https://data.nationalbank.kz/api/report/stat-data-locale'

    load_date = context['ds']
    ti = context['ti']
    d = pendulum.parse(load_date)

    prev_month_first = d.subtract(months=1).start_of("month").to_date_string()

    headers = {
        'Accept': 'application/json'
    }
    params = {
        'form_id': '299',
        'date_from': prev_month_first,
        'date_to': prev_month_first,
        'lang': 'en'
    }

    resp = requests.get(url, headers = headers, params = params)
    data = resp.json()
    df = pd.json_normalize(data)

    #####normlising attributions 
    def attrs_to_dict(attrs):
        if not isinstance(attrs, list):
            return {}
        return {d.get("attrCode"): d.get("valueCode") for d in attrs}

    attrs_df = df["attributes"].apply(attrs_to_dict).apply(pd.Series)

    df = pd.concat([df.drop(columns=["attributes"]), attrs_df], axis=1)
    #######

    #drop unnnecessery columns and change column name and add additional column 
    df = df.drop(columns = ['type', 'category'])
    df = df.rename(columns={'fx_currency_pair': 'currency', 'reportDate': 'report_date'})
    df["updated_at"] = pd.Timestamp.now()

    ##### TO S3:
    filename = f"yerke_kyz/api/currency_exchange/KZT_exchange_{load_date}.parquet"

     # save as Parquet and to s3: 
    if len(df) > 0:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(), key=filename, bucket_name="dev", replace=True
        )

    total_rows = len(df)
    print(f"==== Currency data for {load_date} uploaded!")
    print(f"==== Row number: {total_rows}")

    ti.xcom_push(key="load_date", value=load_date)
    ti.xcom_push(key="total_rows", value=total_rows)


default_args = {
    'owner':'yerke_kyz',
    'start_date': days_ago(360)
}

dag = DAG('yerke_kyz_API_to_s3_currency', default_args=default_args, 
        description="Currency exchange rate for everyday",
        schedule='0 8 1 * *',
        catchup = True,
        tags=["currency", 'api', 's3'], 
        )
    


#task1 creation:
upload = PythonOperator(task_id="upload", python_callable=api_to_s3, dag=dag)

#task2:
send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="yerke_kyz_bt_tg",
    chat_id="{{ var.value.yerke_kyz_chat_id }}", 
    text=(
        "✅ Currency exchange rates for {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "uploaded\n"
        "Number of lines: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)

upload>>send_message_telegram