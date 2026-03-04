from airflow import (
    DAG,
)  # чтобы airflow понял, что файл нужно читать как ацикличный граф
from airflow.operators.python import (
    PythonOperator,
)  # чтобы запускать питоновские функции
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import (
    S3Hook,
)  # чтобы файл из API загружать в хранилище

import requests
import pandas as pd
import time
import random
import io


def api_download(**context):
    ds = context["ds"]
    ti = context["ti"]
    start_date = ds
    end_date = ds
    url = "https://api.open-meteo.com/v1/forecast"
    response = requests.get(
        url,
        params={
            "latitude": 23.0268,
            "longitude": 113.1315,
            "hourly": ["temperature_2m", "relative_humidity_2m", "rain"],
            "start_date": start_date,
            "end_date": end_date,
        },
        timeout=60,
    )

    if response.status_code == 500:
        time.sleep(10 * random.random())
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(
        {
            "datetime": data["hourly"]["time"],
            "temperature_2m": data["hourly"]["temperature_2m"],
            "relative_humidity_2m": data["hourly"]["relative_humidity_2m"],
            "rain": data["hourly"]["rain"],
        }
    )
    df["updated_at"] = pd.Timestamp.utcnow()  # Дата выгрузки данных

    filename = f"fubarbert/fubarbert_weather/foshan_{ds}.parquet"
    if not df.empty:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=filename,
            bucket_name="dev",
            replace=True
        )

    total_rows = len(df)

    ti.xcom_push(key="load_date", value=start_date)
    ti.xcom_push(key="total_rows", value=total_rows)
    
default_args = {
    "owner": "fubarbert",
    "start_date": days_ago(5),
    "retries": 2,
}

dag = DAG(
    dag_id="fubarbert__API_to_S3__weather",
    default_args=default_args,
    schedule_interval="0 10 * * *",  # каждый день в 10:00 UTC
    catchup=True,
    description="API weather to S3",
    tags=["weather", "rain", "humidity", "api", "s3"],
)
upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)



send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="fubarbert_tg",
    chat_id="{{ var.value.FUBARBERT_TELEGRAM_CHAT_ID}}",
    text=("✅Погода за {{ti.xcom_pull(task_ids='upload', key='load_date')}}"
          "загружена\n"
          "Файл: <code>foshan_{{ ti.xcom_pull(task_ids='upload', key='load_date')}}.parquet</code>\n"
          "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows')}}</b>"
          ), 
          dag=dag,
)
# https://api.telegram.org/bot<YourBOTToken>/getUpdates 
upload >> send_message_telegram