"""
# DAG «Moscow Weather 🌤️»

## Загрузка погоды из Open-Meteo API (temp, rain, snow, cloud)
API Docs: https://open-meteo.com/en/docs

### Что делает DAG
- Ежедневно (в 03:00 UTC) загружает погоду за **предыдущий день**
- Локация: **Москва**
- Параметры:
  - "temperature_2m" - температура, 
  - "snowfall" - количество выпавшего снега за предыдущий час, 
  - "snow_depth" - глубина снежного покрова на земле в данный момент, 
  - "rain" - жидкие осадки за предыдущий час, 
  - "cloud_cover" - облачность (сколько процентов неба закрыто облаками)
- Данные сохраняются в S3 в формате **Parquet**

### Режимы работы

#### 🟢 Обычный режим (по расписанию)
- Загружается один день (execution date → ds)
- Создаётся файл:  
  `dev/Denis_Chinese/Weather/moscow_{YYYY-MM-DD}.parquet`  

#### 🟣 Ретро-режим (ручной запуск с конфигом)
- Запускается через **Trigger DAG with config**
- Загружает данные за произвольный диапазон дат
- Создаётся **один** файл с префиксом history:
  `dev/Denis_Chinese/Weather/moscow_history_{start_date}_{end_date}.parquet`    

Пример конфига для ручного запуска:
```json
{
  "start_date": "2026-01-01",
  "end_date":   "2026-01-07"
}
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
import io
import pandas as pd
import requests

def api_download(**context):

    url = "https://api.open-meteo.com/v1/forecast"

    ti = context["ti"]
    ds = context["ds"]
    dag_run = context.get("dag_run")

    conf = (dag_run.conf or {}) if dag_run else {}

    is_history = bool(conf)

    start_date = conf.get("start_date", ds)
    end_date = conf.get("end_date", ds)

    response = requests.get(
        url,
        params = {
            "latitude": 55.7522,
            "longitude": 37.6156,
            "hourly": ["temperature_2m", "snowfall", "snow_depth", "rain", "cloud_cover"],
            "timezone": "Europe/Moscow",
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    response.raise_for_status() 
    data = response.json()
    df = pd.json_normalize(data)

    if is_history:
        filename = f"Denis_Chinese/Weather/moscow_history_{start_date}_{end_date}.parquet"
    else:
        filename = f"Denis_Chinese/Weather/moscow_{ds}.parquet"

    if not df.empty:
        df["update_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    
    print(f"""==== Загружено: {len(df)} строк{
        (len(df) % 10 == 1)*'а' + (len(df) % 10 in (2,3,4))*'и'
        } за период {start_date} — {end_date}""")


# API -> S3 --> telegram, email

default_args = {
    'owner': 'Denis_Chinese',
    'start_date': days_ago(1),
    'retries': 2,
}

dag = DAG(
    dag_id='Denis_Chinese_API_to_S3__weather',
    default_args=default_args,
    schedule_interval='0 3 * * *',  # каждый день в 03:00 UTS (CRON выражение)
    catchup=False,
    description='API weather to S3', # описание в заголовке после названия дага
    tags=['weather', 'temp', 'rain', 'snow', 's3', 'hook']
)

dag.doc_md = __doc__

upload = PythonOperator(
    task_id='upload',
    python_callable=api_download,
    dag=dag
)


# https://api.telegram.org/bot<YourBOTToken>/getUpdates

send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="denis_tg",
    chat_id="{{ var.value.DENIS_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "✅ Погода за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружена\n"
        "Файл: <code>eq_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Количество строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)

# upload >> [send_email, send_message_telegram]
upload >> send_message_telegram
