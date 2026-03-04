import requests
import pandas as pd
import io
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



def download_eq(**context):

    ds = context["ds"]
    ti = context["ti"]
    macros = context['macros']
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    start_date = conf.get('start_date', ds)
    
    # Вызываем метод
    formatted_date = macros.ds_format(start_date, "%Y-%m-%d", "%Y/%m/%d")

    url = f"http://openlibrary.org/recentchanges/{formatted_date}.json?"


    print(f"📡 Загрузка данных за {start_date}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса: {e}")

    if data:
        df = pd.json_normalize(data)
        df['updated_at'] = pd.to_datetime(df['timestamp']).dt.strftime("%Y-%m-%d")
        total_rows = len(df)
        print(f"Количество записей: {total_rows}")
        ti.xcom_push(key="load_date", value=start_date)
        ti.xcom_push(key="total_rows", value=total_rows)
    else:
        print("Изменений не вносилось")
        ti.xcom_push(key="load_date", value=start_date)
        ti.xcom_push(key="total_rows", value=0)
        return []

    filename = f"fut3r/books/books_{start_date}.parquet"

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
        
def task_success_alert(context):

    print(f"Успешное выполнение таска {context['task_instance_key_str']}\nЗагружено {context['ti'].xcom_pull(task_ids='upload', key='total_rows')} ")

def task_failed_alert(context):
    print(f"Таска {context['task_instance_key_str']} упала ")

default_args = {
        "owner": "fut3r", 
        "start_date": days_ago(5),
        "retries": 1
}
with DAG(
    dag_id="fut3r__API_to_S3_books",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    catchup=True,
    description="API books to S3",
    tags=["books", "s3", "airflow"],
) as dag:

    check_api_ready = HttpSensor(
        task_id='check_api_ready',
        http_conn_id='open_library_api',
        endpoint="recentchanges/{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d') }}.json",
        method='GET',
        response_check=lambda response: any(listing.get('kind') == 'add-book' for listing in response.json()),
        poke_interval=600, # Проверять каждые 10 минут
        timeout=3600,       # Ждать максимум час
    )

    upload = PythonOperator(
        task_id="upload",
        python_callable=download_eq,
        on_success_callback=task_success_alert,
        on_failure_callback=task_failed_alert,
        dag=dag
    )



check_api_ready >> upload 