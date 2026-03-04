"""
### DAG "aidarai_API_to_S3_earthquake"

DAG предназначен для ежедневной загрузки данных о землетрясениях из публичного API USGS и сохранения их в S3-хранилище.

**Логика работы:**
- DAG запускается ежедневно в 07:00 с поддержкой догоняющих запусков (`catchup=True`).
- За каждый день выгружаются события землетрясений с магнитудой **5.0 и выше**.
- Данные запрашиваются за период от `execution_date` до `execution_date + 1 день`.
- Ответ API преобразуется в pandas DataFrame, обогащается датой события и временем загрузки.
- Результат сохраняется в формате **Parquet** и загружается в S3 (bucket `dev`) по пути  
  `aidarai/eartquakes/{date}.parquet`.

**Основные компоненты:**
- `PythonOperator` — выполняет функцию `api_download`
- `S3Hook` — используется для загрузки данных в S3
- Источник данных: USGS Earthquake API

DAG рассчитан на надёжную ежедневную загрузку данных с возможностью повторных попыток при ошибках.

"""

import requests
import pandas as pd
import datetime
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.telegram.operators.telegram import TelegramOperator
import io
import logging

def api_download(**context):
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    
    ti = context["ti"]
    update_at = datetime.datetime.now()
    print(update_at)
    load_date_start = context["execution_date"].strftime("%Y-%m-%d")
    print(load_date_start)
    load_date_end = (context["execution_date"] + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    print(load_date_end)
    
    params = {"format": "geojson", 
              "starttime": load_date_start,
              "endtime": load_date_end,
              "minmagnitude": 5.0,
              "orderby": "time"}
              
    response = requests.get(url, params=params)
    response.raise_for_status()

    df = pd.json_normalize(response.json()["features"])

    df.columns = df.columns.str.replace(".", "_", regex=False)

    if not df.empty:

        df["longitude"] = df["geometry_coordinates"].str[0]
        df["latitude"]  = df["geometry_coordinates"].str[1]
        df["depth"]     = df["geometry_coordinates"].str[2]

        df.drop(columns=["geometry_coordinates"], inplace=True)

        df["event_date"] = pd.to_datetime(df["properties_time"], unit="ms", utc=True)
        df["update_at"] = pd.to_datetime(update_at)

        path_s3 = f'aidarai/eartquakes/{load_date_start}.parquet'
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False,  engine="pyarrow", coerce_timestamps='us')
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=path_s3,
            bucket_name="dev",
            replace=True,
        )
    
    ti.xcom_push(key="start_date", value=load_date_start)
    ti.xcom_push(key="end_date", value=load_date_end)
    ti.xcom_push(key="total_rows", value=len(df))

    print(f"==== Загружено {len(df)} строк за период с {load_date_start} по {load_date_end}")

#описываем параметры ДАГа
default_args = {"owner": "aidarai", 
                "start_date": days_ago(7),
                "retries":3
                }
            
dag = DAG(
    dag_id="aidarai_API_to_S3_earthquake",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=True,
    description="API earthquake to S3",
    tags=["earthquakes", "s3", "airflow", "daily"],
)
dag.doc_md = __doc__

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)

send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id='aidarai_tg',
    chat_id="{{ var.value.aidarai_TELEGRAM_CHAT_ID }}",
    text=(
        "✅ Данные за период с {{ ti.xcom_pull(task_ids='upload', key='start_date') }} по {{ ti.xcom_pull(task_ids='upload', key='end_date') }}"
        " загружены\n"
        "Файл: <code>{{ ti.xcom_pull(task_ids='upload', key='start_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)

upload >> send_message_telegram
