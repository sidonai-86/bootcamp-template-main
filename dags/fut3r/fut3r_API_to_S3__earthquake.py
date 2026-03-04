import requests
import pandas as pd
import io
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



def download_eq(**context):

    ds = context["ds"]
    next_ds = context["tomorrow_ds"] 
    ti = context["ti"]
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    start_date = conf.get('start_date', ds)
    end_date = next_ds #datetime.strptime(conf.get('end_date', ds),"%Y-%m-%d")

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    params = {
        "format": "geojson",
        "minmagnitude": 2,
        "starttime": start_date,
        "endtime":   end_date,  #(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        "orderby": "time",
    }
    print(f"📡 Загрузка данных за {start_date}")
    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    eqs = data["features"]
    df = pd.json_normalize(eqs)
    if not df.empty:
        df['created_at'] = pd.to_datetime(df['properties.time'], unit="ms").dt.strftime("%Y-%m-%d")
        df['updated_at'] = datetime.now().strftime("%Y-%m-%d")
        #print(df.head(), df.shape)
        total_rows = len(df)
        print(f"Количество записей: {total_rows}")
        ti.xcom_push(key="load_date", value=start_date)
        ti.xcom_push(key="total_rows", value=total_rows)
    else:
        print("Землетрясений не было")
        ti.xcom_push(key="load_date", value=start_date)
        ti.xcom_push(key="total_rows", value=0)
        return []

    filename = f"fut3r/earthquakes/earthquakes{ds}.parquet"

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

default_args = {
        "owner": "fut3r", 
        "start_date": days_ago(30),
        "retries": 1
}
dag = DAG(
    dag_id="fut3r__API_to_S3_earthquake",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    catchup=True,
    description="API earthquake to S3",
    tags=["earthquake", "s3", "airflow"],
)

upload = PythonOperator(task_id="upload", python_callable=download_eq, dag=dag)
'''
    send_email = EmailOperator(
        task_id="send_email",
        to="futer90@yandex.ru",
        subject="Данные по землетрясениям загружены за {{ ti.xcom_pull(task_ids='upload', key='load_date') }}",
        html_content="""
            <p>Файл <b>earthquake_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</b> успешно положен в S3.</p>
            <p>Путь: <code>s3://dev/api/fut3r/earthquakes/earthquakes_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code></p>
            <p>Кол-во строк: {{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</p>
        """,
        conn_id="smtp_fut3r_yandex",
        dag=dag,
    )
'''
send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="fut3r_tg",
    chat_id="{{ var.value.fut3r_chat_id }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "✅ Землетрясения за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружены\n"
        "Файл: <code>earthquakes_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)


upload >> send_message_telegram 