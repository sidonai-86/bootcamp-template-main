import io
import pandas as pd
import requests
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

logger = logging.getLogger("airflow.task")

def earthquake_to_s3(**context):
    ti = context["ti"]
    ds = context["ds"]
    
    # 1. Запрос
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": ds,
        "endtime": (pd.to_datetime(ds) + timedelta(days=1)).strftime('%Y-%m-%d'),
        "minmagnitude": 0.5 
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    features = data.get("features", [])
    if not features:
        logger.info(f"No data for {ds}")
        ti.xcom_push(key="row_count", value=0)
        return

    # 2. Трансформация
    df = pd.json_normalize(features)
    df.columns = [col.replace('.', '_') for col in df.columns]
    
    if 'properties_time' in df.columns:
        df['properties_time'] = pd.to_datetime(df['properties_time'], unit='ms').dt.floor('us')
    df["update_at"] = pd.Timestamp.now().floor('us')

    # 3. Запись в Parquet
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow', coerce_timestamps='us')
    buffer.seek(0)

    filename = f"sk8california/earthquakes/daily/{ds}.parquet"

    # 4. Загрузка в S3
    s3_hook = S3Hook(aws_conn_id="minios3_conn")
    s3_hook.load_bytes(
        bytes_data=buffer.read(),
        key=filename,
        bucket_name="dev",
        replace=True,
    )

    # 5. Пушим данные для телеги
    ti.xcom_push(key="row_count", value=len(df))
    ti.xcom_push(key="s3_path", value=f"dev/{filename}")

# --- DAG Definition ---

default_args = {
    "owner": "sk8california",
    "start_date": days_ago(7),
    "retries": 1,
}

with DAG(
    dag_id="sk8california__earthquake_final_v3",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=True,
    tags=["earthquake", "API", "S3", "daily"],
) as dag:

    upload = PythonOperator(
        task_id="upload",
        python_callable=earthquake_to_s3,
    )

    notify = TelegramOperator(
        task_id="notify",
        telegram_conn_id="sk_telegram",
        chat_id="{{ var.value.SK_TELEGRAM_CHAT_ID }}",
        text="""
✅ Earthquake Task Success
📅 Date: {{ ds }}
📊 Rows: {{ ti.xcom_pull(task_ids='upload', key='row_count') }}
📂 S3: {{ ti.xcom_pull(task_ids='upload', key='s3_path') }}
"""
    )

    upload >> notify