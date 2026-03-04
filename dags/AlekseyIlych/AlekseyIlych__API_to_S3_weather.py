from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import io
import pandas as pd
import requests
from datetime import datetime, timedelta


def download_api(**context):

    ds = context["ds"]
    ti = context["ti"]

    url = 'https://api.open-meteo.com/v1/forecast'

    latitude = 52.0333
    longitude = 47.7833
    city = 'Balakovo'

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "snowfall"],
        "start_date": ds,
        "end_date": ds
    }

    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Ошибка при запросе к сайту: {e}")

    data = response.json()
    hourly = data.get("hourly", {})
    df = pd.DataFrame({
        "time": hourly.get("time", []),
        "temperature_2m": hourly.get("temperature_2m", []),
        "snowfall": hourly.get("snowfall", []),
    })

    filename = f"AlekseyIlych/API_to_S3/Balakovo_{ds}.parquet"
    if len(df) > 0:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(), key=filename, bucket_name="dev", replace=True
        )

    total_rows = len(df)
    print(f"==== Данные по погоде загружены за {ds}")
    print(f"==== Кол-во строк {total_rows}")

    ti.xcom_push(key="load_date", value=ds)
    ti.xcom_push(key="total_rows", value=total_rows)


default_args = {
    "owner": "AlekseyIlych",
    "start_date": days_ago(2),
    "retries": 2,
}


dag = DAG(
    dag_id="AlekseyIlych_API_to_S3_weather",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=True,
    description="API weather to S3",
    tags=["weather", "temp", "snowfall", "api", "s3"],
)

upload = PythonOperator(task_id="upload", python_callable=download_api, dag=dag)

send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="AlekseyIlych_tg",
    chat_id="{{ var.value.ALEKSEYILYCH_TELEGRAM_CHAT_ID }}",
    text=(
        "✅ Погода за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружена\n"
        "Файл: <code>Balakovo_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)


upload >> send_message_telegram
