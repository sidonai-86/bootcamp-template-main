"""
# DAG «Информация о землетрясениях»
## Получаем данные из API Open-Meteo
    API Docs: "https://earthquake.usgs.gov/fdsnws/event/1/query"
- Данные загружаются инкрементально один раз в день в 10:00 за прошедшую неделю
- Чтобы перезагрузить ретро данные -> Clear Task
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import io
import logging
import pandas as pd
import requests
from datetime import datetime, timedelta

logger = logging.getLogger("airflow.task")

def api_download(**context):

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    #load_date = context["ds"]
    #ti = context["ti"]

    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
        
    params = {
            'format': 'geojson',
            'starttime': start_date.strftime('%Y-%m-%d'),
            'endtime': end_date.strftime('%Y-%m-%d'),
            'minmagnitude': 1.0,
            'orderby': 'time',
            'limit': 1000
        }

    response = requests.get(url, params=params) 
    data = response.json()

    earthquakes = []

    for feature in data['features']:
        props = feature['properties']
        geometry = feature['geometry']
                
        earthquake = {
            'time': pd.to_datetime(props['time'], unit='ms'),
            'latitude': geometry['coordinates'][1],
            'longitude': geometry['coordinates'][0],
            'depth_km': geometry['coordinates'][2],
            'magnitude': props['mag'],
            'place': props['place'],
            'type': props['type'],
            'status': props['status'],
            'tsunami': props['tsunami'],
            'significance': props['sig'],
            'url': props['url'],
            'load_dt': datetime.now().strftime('%Y-%m-%d')
            }

        earthquakes.append(earthquake)


    filename = f"Vsailii1611/earthquakes_world_{end_date.strftime('%Y-%m-%d')}.parquet"

    df = pd.DataFrame(earthquakes)

    logger.info(f"✅ Загружено {len(df)} записей за {end_date.strftime('%Y-%m-%d')}")

    # Сохранить в CSV
    # if len(df) > 0:
    #     # Используем S3Hook
    #     hook = S3Hook(aws_conn_id='minios3_conn')
    #     hook.load_string(
    #         string_data=df.to_csv(index=False, sep=';'),
    #         key=filename,
    #         bucket_name='dev',
    #         replace=True
    #     )

    # Сохранить в Parquet
    if len(df) > 0:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)  # возвращаемся в начало буфера

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(), key=filename, bucket_name="dev", replace=True
        )

        logger.info(f"✅ Файл {filename} загружен в S3")

    total_rows = len(df)
    print(f"==== Данные от {end_date} по землеьтрясениям за прошедшую неделю")
    print(f"==== Кол-во строк {total_rows}")

    #ti.xcom_push(key="end_date", value=load_date)
    #ti.xcom_push(key="total_rows", value=total_rows)


with DAG (
    dag_id='Vasilii1611_API_to_S3__earthquakes',
    start_date = datetime(2026, 1, 1),
    schedule_interval="0 10 * * *",  # каждый день в 10:00 UTC
    catchup=False,
    tags=["earthquakes", "api", "s3"],
    description="API earthquakes to S3"
    ) as dag:

    upload_task = PythonOperator(task_id="upload_task", python_callable=api_download)

    upload_task