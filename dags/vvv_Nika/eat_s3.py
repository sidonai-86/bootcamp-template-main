"""
# DAG «Eatherquake»

## Получаем данные из API eatherquake
    API Docs: https://earthquake.usgs.gov/fdsnws/event/1/query

- Данные загружаются инкрементально один раз в день за вчера в 4:00
- Чтобы перезагрузить ретро данные -> Clear Task
"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import io
import pandas as pd
import requests
from datetime import datetime
import pyarrow


def api_download(**context):
   
    load_date = context["ds"]
    ti = context["ti"]

    start_date = f"{load_date}T00:00:00"
    end_date = f"{load_date}T23:59:59"    

    update_at = datetime.now()

    response = requests.get(
        "https://earthquake.usgs.gov/fdsnws/event/1/query",
        params={
            "format": "geojson",
            "starttime": start_date,
            "endtime": end_date
        }, timeout=60,
    )

    response.raise_for_status()
    data = response.json()

    features = data.get('features', [])
    df = pd.json_normalize(features)

    filename = f"vvv_Nika/eatherquake_to_S3/{load_date}.parquet"
    
    df["update_at"] = update_at
    df["bisness_date"] = load_date


    if not df.empty:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=filename,
            bucket_name="dev",
            replace=True,
        ) 

    total_rows = len(df)

    print(f"==== Данные загружены за {load_date}")
    print(f"==== Кол-во строк {total_rows}")

    ti.xcom_push(key="load_date", value=load_date)
    ti.xcom_push(key="total_rows", value=total_rows)



default_args = {
    "owner": "vvv_Nika",
    "start_date": days_ago(10),
    "retries": 3,
}

dag = DAG(
    dag_id="vvv_eatherquake_to_S3",
    default_args=default_args,
    schedule_interval="0 4 * * *",  # каждый день в 04:00 UTC
    catchup=True,
    description="description_eatherquake",
    tags=["eatherquake", "api", "s3"],
)

dag.doc_md = __doc__

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)


# https://api.telegram.org/bot<YourBOTToken>/getUpdates
send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="nika_tg",
    chat_id="{{ var.value.NIKA_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "✅ Eatherquake за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружена\n"
        "Файл: <code>{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)

upload >> send_message_telegram