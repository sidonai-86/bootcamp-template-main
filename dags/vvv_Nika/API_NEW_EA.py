from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.telegram.operators.telegram import TelegramOperator 
from datetime import datetime, timedelta
import io
import requests
import pandas as pd
from airflow.utils.dates import days_ago 

def download_and_save_earthquakes(**context):
    exec_date = context["ds"]
    exec_dt = datetime.strptime(exec_date, "%Y-%m-%d")
    yesterday = exec_dt - timedelta(days=1)
    yesterday_date = yesterday.strftime("%Y-%m-%d")

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",           
        "starttime": f"{yesterday_date}T00:00:00",  
        "endtime": f"{yesterday_date}T23:59:59",    
        "minmagnitude": 4.0,          
        "orderby": "magnitude",        
        "limit": 100                  
    }
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    features = data.get('features', [])
    print(f"Найдено: {len(features)} землетрясений")

    earthquakes_list = []
    for quake in features:
        earthquakes_list.append({
            'magnitude': quake['properties']['mag'],
            'place': quake['properties']['place'],
            'time_ms': quake['properties']['time'],
            'longitude': quake['geometry']['coordinates'][0],
            'latitude': quake['geometry']['coordinates'][1],
            'depth_km': quake['geometry']['coordinates'][2],
        })
    
    df = pd.DataFrame(earthquakes_list)
    
    # ИСПРАВЛЕНИЕ: Сохраняем время как строку
    df['time_utc'] = pd.to_datetime(df['time_ms'], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['business_date'] = yesterday_date
    df = df.drop('time_ms', axis=1, errors='ignore')

    filename = f"NN_earthquakes/{yesterday_date}.parquet"

    buffer = io.BytesIO()
    
    # ИСПРАВЛЕНИЕ: Параметры для безопасного сохранения
    df.to_parquet(
        buffer, 
        index=False,
        coerce_timestamps='ms',
        allow_truncated_timestamps=True
    ) 
    
    buffer.seek(0)

    file_size = buffer.getbuffer().nbytes
    print(f"Размер файла: {file_size} байт")

    hook = S3Hook(aws_conn_id="minios3_conn")
    hook.load_bytes(
        bytes_data=buffer.read(),  
        key=filename, 
        bucket_name="dev",  
        replace=True,  
    )

    context['ti'].xcom_push(key='filename', value=filename)
    context['ti'].xcom_push(key='count', value=len(df))
    context['ti'].xcom_push(key='date', value=yesterday_date)  
    return len(df)


default_args = {
    "owner": "sssnnn", 
    "start_date": days_ago(2),  
    "retries": 2,
}

dag = DAG(
    dag_id="nn_earthquake_dag", 
    default_args=default_args,
    schedule_interval="0 5 * * *",  
    catchup=True,
    description="Загрузка землетрясений",
    tags=["earthquake"],
) 


upload = PythonOperator(
    task_id="upload", 
    python_callable=download_and_save_earthquakes,  
    dag=dag
)


send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="nik_new_api",
    chat_id="{{ var.value.nik_api_new }}",
    text="""
✅ Загружено {{ ti.xcom_pull(task_ids='upload', key='count') }} строк за {{ ti.xcom_pull(task_ids='upload', key='date') }}
Формат: Parquet
Файл: {{ ti.xcom_pull(task_ids='upload', key='filename') }}
""",
    dag=dag,
)

upload >> send_message_telegram