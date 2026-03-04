from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import io
import pandas as pd
import requests
from datetime import datetime


def api_download(**context):

    ds=context["ds"]

    ti = context["ti"]

    url = "https://api.open-meteo.com/v1/forecast"

    update_date = datetime.now()
    start_date = ds
    end_date = ds

    response = requests.get(
        url,
        params={
            "latitude": 53.9,
            "longitude": 27.5667,
            "hourly": ["temperature_2m", "rain", "snowfall"],
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    response.raise_for_status()

    data = response.json()

    hourly = data.get("hourly", {})
    df = pd.DataFrame({
        "time": hourly.get("time", []),
        "temperature_2m": hourly.get("temperature_2m", []),
        "rain": hourly.get("rain", []),
        "snow": hourly.get("snowfall", []),
    })
    df['update_at'] = update_date


    filename = f"business2077/API_to_S3__weather_temp_rain_snow_config/Minsk{ds}.parquet"

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

    ti.xcom_push(key="load_date", value=start_date)
    ti.xcom_push(key="total_rows", value=total_rows)

default_args = {
"owner": "business2077",
"start_date": days_ago(3),
"retries": 2,
}

dag = DAG(
    dag_id="business__API_to_S3__weather_temp_rain_snow_config",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # каждый день в 03:00 UTC
    catchup=True,#если true то запускаются все прошедшие даты начиная с начала расписания, false-будет запускать начиная с последнего
    description="dag description",
    tags=["weather", "temp", "rain","snow", "api", "s3"],
)

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)

send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="busines2077_tg",
    chat_id="{{ var.value.BUSINESS2077_TELEGRAM_CHAT_ID }}",  
    text=(
        "✅ Погода за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружена\n"
        "Файл: <code>minsk_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)


upload >> send_message_telegram



