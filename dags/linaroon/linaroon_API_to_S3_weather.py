from __future__ import annotations

import io
import logging
from datetime import datetime

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.telegram.operators.telegram import TelegramOperator


BUCKET = "dev"
USER_PREFIX = "linaroon"
DATASET_PREFIX = "weather"
TZ = "America/New_York"
LAT = 40.71427                    # NYC
LON = -74.00597


URL_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARS = ["temperature_2m", "precipitation", "rain", "snowfall", "weather_code"]


def api_download(**context):
    ti = context["ti"]
    ds: str = context["ds"] 

    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    is_history = bool(conf)

    start_date = conf.get("start_date", ds)
    end_date = conf.get("end_date", ds)

    response = requests.get(
        URL,
        params = {
            "latitude": LAT,
            "longitude": LON,
            "hourly": HOURLY_VARS,
            "timezone": TZ,
            "start_date": start_date,
            "end_date": end_date,
        },
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()

    # --- normalize hourly ---
    hourly = data.get("hourly", {})
    df = pd.DataFrame({"time": hourly.get("time", [])})
    for v in HOURLY_VARS:
        df[v] = hourly.get(v, [])

    if is_history:
        business_date = None
        filename = f"{USER_PREFIX}/{DATASET_PREFIX}/history_{start_date}_{end_date}.parquet"
    else:
        business_date = ds  # или start_date, но без conf это == ds
        df["business_date"] = business_date
        filename = f"{USER_PREFIX}/{DATASET_PREFIX}/{business_date}.parquet"


    df["update_at"] = datetime.utcnow().isoformat()

    total_rows = len(df)

    # --- logs ---
    logging.info("Loaded %s rows for period %s — %s", total_rows, start_date, end_date)
    print(f"==== Загружено {total_rows} строк за период {start_date} — {end_date}")

    # --- upload ---
    if not df.empty:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=filename,
            bucket_name=BUCKET,
            replace=True,
        )

        logging.info("Uploaded to s3://%s/%s", BUCKET, filename)
        print(f"==== Uploaded to s3://{BUCKET}/{filename}")
    else:
        logging.warning("Empty dataframe; nothing uploaded")

    # --- xcom for possible notifications ---
    ti.xcom_push(key="start_date", value=start_date)
    ti.xcom_push(key="total_rows", value=total_rows)
    ti.xcom_push(key="s3_key", value=filename)


default_args = {
    "owner": "linaroon",
    "start_date": days_ago(7),
    "retries": 2,
}


dag = DAG(
    dag_id="linaroon_API_to_S3_weather",
    default_args=default_args,
    schedule_interval="0 3 * * *",  
    catchup=True,
    description="NYC hourly weather -> MinIO S3",
    tags=["weather", "temp", "api", "s3"],
)

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)


# https://api.telegram.org/bot<YourBOTToken>/getUpdates
send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="linaroon_tg",
    chat_id="{{ var.value.LINAROON_TELEGRAM_CHAT_ID }}",
    text=(
        "✅ NYC weather выгрузка завершена\n"
        "Период: <b>{{ ti.xcom_pull(task_ids='upload', key='start_date') }}</b>\n"
        "Строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>\n"
        "S3: <code>s3://dev/{{ ti.xcom_pull(task_ids='upload', key='s3_key') }}</code>"
    ),
    dag=dag,
)

upload >> send_message_telegram