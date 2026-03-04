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

def weather_to_s3(**context):
    ti = context["ti"]
    ds = context["ds"]
    
    # 1. Список городов
    cities = {
        "Moscow": {"lat": 55.7558, "lon": 37.6173},
        "Batumi": {"lat": 41.6423, "lon": 41.6339},
        "Izhevsk": {"lat": 56.8498, "lon": 53.2045},
        "Kazan": {"lat": 55.7887, "lon": 49.1221}
    }
    
    all_frames = []

    for city_name, coords in cities.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": ds,
            "end_date": ds,
            "hourly": "temperature_2m,relative_humidity_2m,surface_pressure,wind_speed_10m",
            "timezone": "UTC"
        }

        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            hourly_data = data.get("hourly", {})
            if hourly_data:
                temp_df = pd.DataFrame(hourly_data)
                temp_df["city"] = city_name  # Добавляем идентификатор города
                all_frames.append(temp_df)
        except Exception as e:
            logger.error(f"Error fetching data for {city_name}: {e}")

    if not all_frames:
        logger.info(f"No weather data collected for {ds}")
        ti.xcom_push(key="row_count", value=0)
        return

    # 2. Трансформация (склеиваем все города)
    df = pd.concat(all_frames, ignore_index=True)
    df.columns = [col.replace('.', '_') for col in df.columns]
    
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time']).dt.floor('us')
    df["update_at"] = pd.Timestamp.now().floor('us')

    # 3. Запись в Parquet
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow', coerce_timestamps='us')
    buffer.seek(0)

    filename = f"sk8california/weather/daily/{ds}.parquet"

    # 4. Загрузка в S3
    s3_hook = S3Hook(aws_conn_id="minios3_conn")
    s3_hook.load_bytes(
        bytes_data=buffer.read(),
        key=filename,
        bucket_name="dev",
        replace=True,
    )

    ti.xcom_push(key="row_count", value=len(df))
    ti.xcom_push(key="s3_path", value=f"dev/{filename}")

# --- DAG Definition ---

default_args = {
    "owner": "sk8california",
    "start_date": days_ago(3),
    "retries": 1,
}

with DAG(
    dag_id="sk8california__weather_to_s3",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=True,
    tags=["weather", "API", "S3", "daily"],
) as dag:

    upload = PythonOperator(
        task_id="upload",
        python_callable=weather_to_s3,
    )

    notify = TelegramOperator(
        task_id="notify",
        telegram_conn_id="sk_telegram",
        chat_id="{{ var.value.SK_TELEGRAM_CHAT_ID }}",
        text="""
✅ Weather Task Success
📅 Date: {{ ds }}
📊 Total Rows (4 cities): {{ ti.xcom_pull(task_ids='upload', key='row_count') }}
📂 S3: {{ ti.xcom_pull(task_ids='upload', key='s3_path') }}
"""
    )

    upload >> notify