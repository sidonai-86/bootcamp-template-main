"""
# DAG «Earthquakes 🌍»

## Загрузка данных о землетрясениях из USGS API
API Docs: https://earthquake.usgs.gov/fdsnws/event/1/

### Что делает DAG
- Ежедневно (в 03:00 UTC) загружает землетрясения за **предыдущий день**
- Источник: USGS FDSN Event Web Service
- Формат: **GeoJSON → pandas → Parquet**
- Данные сохраняются в S3 (bucket: dev)

### Режимы работы

#### 🟢 Обычный режим (по расписанию)
- Загружается один день (execution date → ds)
- Фильтр: starttime = {ds}T00:00:00   endtime = {ds}T23:59:59
- Создаётся файл:  
  `dev/Denis_Chinese/Earthquakes/eq_{YYYY-MM-DD}.parquet`  

#### 🟣 Ретро-режим (ручной запуск с конфигом)
- Запускается через **Trigger DAG with config**
- Загружает данные за произвольный диапазон дат
- Создаётся **один** файл с префиксом history:

Пример конфига для ручного запуска:
```json
{
  "start_date": "2025-12-01",
  "end_date":   "2025-12-05"
}
"""

import requests
from datetime import datetime
import pandas as pd
import io

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def api_download(**context):

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    ti = context["ti"]
    ds = context["ds"]
    dag_run = context.get("dag_run")

    conf = (dag_run.conf or {}) if dag_run else {}

    is_history = bool(conf)

    start_date = conf.get("start_date", ds)
    end_date = conf.get("end_date", ds)
    
    print(f"Execution date: {ds}")

    response = requests.get(
        url,
        params={
            "format": "geojson",
            "endtime": f"{end_date}T23:59:59",
            "starttime": f"{start_date}T00:00:00"
        },
    )

    response.raise_for_status()

    data = response.json()
    features = data.get("features", [])

    # if not features:
    #     print("No earthquakes found for this day.")
    #     return

    quakes = [feature['properties'] for feature in data['features']]
    df = pd.json_normalize(quakes)

    if is_history:
        filename = f"Denis_Chinese/Earthquakes/eq_history_{start_date}_{end_date}.parquet"
    else:
        filename = f"Denis_Chinese/Earthquakes/eq_{ds}.parquet"

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
    
    print(f"==== Загружено {len(df)} строк(и) за период {start_date} — {end_date}")


# API -> S3 --> telegram, email

default_args = {
    'owner': 'Denis_Chinese',
    'start_date': days_ago(1),
    'retries': 2,
}

dag = DAG(
    dag_id='Denis_Chinese_API_to_S3__eq',
    default_args=default_args,
    schedule_interval='0 3 * * *',  # каждый день в 03:00 UTS (CRON выражение)
    catchup=False,
    description='API eq to S3', # описание в заголовке после названия дага
    tags=['eq', 's3', 'hook']
)

dag.doc_md = __doc__

upload = PythonOperator(
    task_id='upload',
    python_callable=api_download,
    dag=dag
)

# send_email = EmailOperator(
#     task_id="send_email",
#     to="den_1301@mail.ru",
#     subject="Eq parquet загружен за {{ ti.xcom_pull(task_ids='upload', key='load_date') }}",
#     html_content="""
#         <p>Файл <b>eq_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</b> успешно положен в S3.</p>
#         <p>Путь: <code>s3://dev/Denis_Chinese/Earthquakes/eq_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code></p>
#         <p>Кол-во строк: {{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</p>
#     """,
#     conn_id="smtp_denis_mail",
#     dag=dag,
# )

# https://api.telegram.org/bot<YourBOTToken>/getUpdates

send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="denis_tg",
    chat_id="{{ var.value.DENIS_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "✅ Землетрясения за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружены\n"
        "Файл: <code>eq_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)

# upload >> [send_email, send_message_telegram]
upload >> send_message_telegram