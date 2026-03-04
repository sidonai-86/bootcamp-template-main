"""
# DAG «Moscow Weather (temp, rain)»

## Загрузка погоды из Open-Meteo API
API Docs: https://open-meteo.com/en/docs

### Что делает DAG
- Ежедневно (в 10:00 UTC) загружает погоду за **предыдущий день**
- Локация: **Москва**
- Параметры: температура (`temperature_2m`) и осадки (`rain`)
- Данные сохраняются в S3 в формате **Parquet**

### Режимы работы

#### 🟢 Обычный режим (по расписанию)
- Загружается один день (`ds`)
- Создаётся файл: moscow_YYYY-MM-DD.parquet

#### 🟣 Ретро-режим (ручной запуск)
- Запускается через **Trigger DAG with config**
- Загружает данные за указанный диапазон дат
- Создаётся один файл с префиксом `history`:

Пример конфига:
```json
{
"start_date": "2026-01-01",
"end_date": "2026-01-10"
}

"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import io
import pandas as pd
import requests


def api_download(**context):
    ti = context["ti"]

    ds = context["ds"]
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    is_history = bool(conf)

    start_date = conf.get("start_date", ds)
    end_date = conf.get("end_date", ds)

    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": 55.7522,
            "longitude": 37.6156,
            "hourly": ["temperature_2m", "rain"],
            "start_date": start_date,
            "end_date": end_date,
        },
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()

    # почасовая нормализация
    hourly = data.get("hourly", {})
    df = pd.DataFrame({
        "time": hourly.get("time", []),
        "temperature_2m": hourly.get("temperature_2m", []),
        "rain": hourly.get("rain", []),
    })

    if is_history:
        filename = f"api/weather_temp_rain_config/moscow_history_{start_date}_{end_date}.parquet"
    else:
        filename = f"api/weather_temp_rain_config/moscow_{ds}.parquet"

    if not df.empty:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=filename,
            bucket_name="prod",
            replace=True,
        )

    ti.xcom_push(key="start_date", value=start_date)
    ti.xcom_push(key="end_date", value=end_date)
    ti.xcom_push(key="total_rows", value=len(df))

    print(f"==== Загружено {len(df)} строк за период {start_date} — {end_date}")

default_args = {
    "owner": "loader",
    "start_date": days_ago(30),
    "retries": 2,
}


dag = DAG(
    dag_id="DEMO__API_to_S3__weather_temp_rain_config",
    default_args=default_args,
    schedule_interval="0 10 * * *",  # каждый день в 10:00 UTC
    catchup=True,
    description="API weather to S3",
    tags=["weather", "temp", "rain", "api", "s3"],
)

dag.doc_md = __doc__

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)


send_email = EmailOperator(
    task_id="send_email",
    to="halltape@yandex.ru",
    subject="Weather parquet загружен за {{ ti.xcom_pull(task_ids='upload', key='load_date') }}",
    html_content="""
        <p>Файл <b>moscow_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</b> успешно положен в S3.</p>
        <p>Путь: <code>s3://dev/api/halltape_vindyukov/moscow_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code></p>
        <p>Кол-во строк: {{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</p>
    """,
    conn_id="smtp_halltape_yandex",
    dag=dag,
)


# https://api.telegram.org/bot<YourBOTToken>/getUpdates
send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="halltape_tg",
    chat_id="{{ var.value.HALLTAPE_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "✅ Погода за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружена\n"
        "Файл: <code>moscow_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)


upload >> [send_email, send_message_telegram]
