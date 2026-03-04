# Импортируем необходимые библиотеки
import requests
from datetime import datetime, timedelta
import logging
import pandas as pd
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger("airflow.task")

def load_earthquake_from_api_to_S3(**context):
    # Устанавливаем URL для API
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    starttime = context["execution_date"].strftime("%Y-%m-%d")
    endtime = (context["execution_date"] + timedelta(days=1)).strftime("%Y-%m-%d")

    # Параметры запроса
    params = {
        'format': 'geojson', # формат выгрузки
        'starttime': starttime,
        'endtime': endtime,
        'eventtype': 'earthquake', # Тип события
        'minmagnitude': 2.5, # Магнитуда
        'limit': 100, # Кол-во событий
        'orderby': "time" # Сортировка по дате
    }
    
    records = []
    # Выполнение запроса
    response = requests.get(url, params=params)  # Исправлено: requests вместо req
    data = response.json()
    
    # Парсинг файла json для последующей записи в формате csv
    for feature in data['features']:
        record = {
            'event_id': feature['id'],
            'magnitude': feature['properties']['mag'],
            'place': feature['properties']['place'],
            'time': datetime.fromtimestamp(feature['properties']['time']/1000).strftime('%Y-%m-%d %H:%M:%S'),
            'load_dt': datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Добавление даты выгрузки
        }
        records.append(record)

    # Создаем DataFrame
    df = pd.DataFrame(records)

    logger.info(f"✅ Загружено {len(df)} записей за {starttime}")

    # Конвертация в Parquet
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
    parquet_buffer.seek(0)
    
    execution_date = context["execution_date"]
    filename = f"amelnikov109/earthquakes_{execution_date.strftime('%Y%m%d')}.parquet"

    # Используем S3Hook
    hook = S3Hook(aws_conn_id='minios3_conn')
    hook.load_bytes(
        bytes_data=parquet_buffer.read(),  
        key=filename, 
        bucket_name="dev", 
        replace=True
    )
    
    logger.info(f"✅ Файл {filename} загружен в S3")

default_args = {
    "owner": "loader", 
    "start_date": days_ago(7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="amelnikov109__API_to_S3_earthquake",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True,
    description="API earthquake to S3",
    tags=["earthquake", "s3", "airflow"],
    max_active_runs=1,
)

load_earthquake_task = PythonOperator(  
    task_id="load_to_s3",  
    python_callable=load_earthquake_from_api_to_S3, 
    dag=dag,
    provide_context=True  
)

load_earthquake_task