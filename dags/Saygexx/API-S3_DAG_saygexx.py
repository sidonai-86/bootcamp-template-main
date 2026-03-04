from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.telegram.operators.telegram import TelegramOperator

import pandas as pd
import requests


def api_download(**context):
    # Получаем служебные объекты Airflow
    ti = context["ti"]              # для XCom
    load_date = context["ds"]       # дата запуска DAG (YYYY-MM-DD)

    # Запрос к погодному API
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",  # Оборачиваем запрос к API
        params={
            "latitude": 58.0105,
            "longitude": 56.2502,
            "hourly": ["temperature_2m", "snowfall"],  # почасовые данные
            "timezone": "auto",
            "start_date": load_date,
            "end_date": load_date,
        },
        timeout=60,
    )
    response.raise_for_status()     # падаем, если API вернул ошибку
    data = response.json()          # JSON → dict

    # Почасовая нормализация данных
    hourly = data.get("hourly", {})

    df = pd.DataFrame(
        {
            "event_time": pd.to_datetime(hourly.get("time", []), utc=True),
            "temperature_2m": hourly.get("temperature_2m", []),
            "snowfall_mm": hourly.get("snowfall", []),
        }
    )

    # Проверка: если данных нет — останавливаем DAG
    if df.empty:
        raise ValueError("No hourly weather data received")

    # Обогащение техническими полями
    df["load_date"] = load_date     # дата загрузки
    df["city"] = "Perm"             # город
    df["source"] = "open_meteo"     # источник данных

    # Сохраняем данные локально в Parquet
    file_path = f"/tmp/weather_{load_date}.parquet"
    df.to_parquet(file_path, index=False)

    # Формируем путь файла в S3 (partition по дате)
    s3_key = f"weather/hourly/load_date={load_date}/weather.parquet"

    # Загружаем файл в S3
    s3 = S3Hook(aws_conn_id="minios3_conn")
    s3.load_file(
        filename=file_path,
        key=s3_key,
        bucket_name="prod",
        replace=True,
    )

    # Передаём метаданные в XCom для уведомлений
    ti.xcom_push(key="load_date", value=load_date)
    ti.xcom_push(key="rows_count", value=len(df))   # сколько строк
    ti.xcom_push(key="s3_key", value=s3_key)        # путь в S3


# Параметры DAG по умолчанию
default_args = {
    "owner": "saygexx",
    "start_date": days_ago(2),
    "retries": 2,
}

# Описание DAG и расписание
with DAG(
    dag_id="API_to_S3_weather_hourly",
    default_args=default_args,
    schedule_interval="0 10 * * *",  # каждый день в 10:00 UTC
    catchup=True,
    description="Hourly weather data from Open-Meteo to S3",
    tags=["weather", "hourly", "snowfall", "api", "s3"],
) as dag:

    # Таска загрузки данных
    upload = PythonOperator(
        task_id="upload_weather_to_s3",
        python_callable=api_download,
    )

    # Отправка уведомления в Telegram
    send_message_telegram = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id="saygexx_tg",
        chat_id="{{ var.value.saygexx_chat_id }}",
        text=(
            "✅ <b>Погода загружена</b>\n\n"
            "📅 Дата: {{ ti.xcom_pull(task_ids='upload_weather_to_s3', key='load_date') }}\n"
            "📦 Кол-во строк: {{ ti.xcom_pull(task_ids='upload_weather_to_s3', key='rows_count') }}\n"
            "📁 Файл:\n"
            "<code>s3://prod/{{ ti.xcom_pull(task_ids='upload_weather_to_s3', key='s3_key') }}</code>"
        ),
    )

    # Порядок выполнения тасок
    upload >> send_message_telegram
