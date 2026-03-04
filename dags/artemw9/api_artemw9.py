from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import requests
import pyarrow
import pandas as pd
import datetime as dt
import io

load_date = dt.date.today()
url = "https://api.open-meteo.com/v1/forecast"
    
  
def get_api_data(**context):

    load_date = context["ds"]
    ti = context["ti"]  

    response = requests.get( 
    url,  
    params = {   
        "latitude" : 45.3,
        "longitude" : 75.45,
        "hourly" : ["cloud_cover" ,"surface_pressure","weather_code"],
        "start_date" : "2025-12-01",
        "end_date" : "2026-01-01" 
    } )

    response.raise_for_status()
    data = response.json()

    ti.xcom_push(key="api_data", value=data)
    ti.xcom_push(key="load_date", value=load_date)
  
def filter_values (**context):

  ti = context["ti"]
  load_date = ti.xcom_pull(task_ids='upload', key='load_date')
  data = ti.xcom_pull(task_ids = 'upload', key = 'api_data')

  filename_parquet= f"artemw9/api_artemw9_response_{load_date}.parquet"

  df = pd.DataFrame({
    'datetime':data['hourly']['time'],
    'cloud_cover':data['hourly']["cloud_cover"],
    'surface_pressure': data['hourly']["surface_pressure"],
    'weather_code': data['hourly']["weather_code"],

})
  df["upload_date"] = load_date
  df = pd.json_normalize(data)

  if len(df) > 0:
      buffer = io.BytesIO()
      df.to_parquet(buffer, index=False)
      buffer.seek(0)  # возвращаемся в начало буфера

      hook = S3Hook(aws_conn_id="minios3_conn")
      hook.load_bytes (
        bytes_data=buffer.read(), key=filename_parquet, bucket_name="dev", replace=True
        )

  total_rows = len(df)

  ti.xcom_push(key="load_date", value=load_date)
  ti.xcom_push(key="total_rows", value=total_rows)


  print(f"==== Данные по погоде загружены за {load_date}")
  print(f"==== Кол-во строк {total_rows}")
    
default_args = {
    "owner": "loader",
    "start_date": days_ago(30),
    "retries": 5,
}

dag = DAG(
    dag_id="artemw9_belyakov_API_to_S3__weather",
    default_args=default_args,
    schedule="0 0 * * *",  # каждый день в 00:00 UTC
    catchup=True,
    description="API data to S3",
    tags=["weather", "clouds", "pressure", "code", "s3"],
)

dag.doc_md = __doc__

upload = PythonOperator(task_id="upload", python_callable=get_api_data, dag=dag)

filter_task = PythonOperator(task_id="filter_values", python_callable=filter_values, dag=dag)

send_email = EmailOperator(
    task_id="send_email",
    to="artembelakov66@gmail.com",
    subject="Weather parquet загружен за {{ ti.xcom_pull(task_ids='filter_values', key='load_date') }}",
    html_content="""
        <p>Файл <b>kz_{{ ti.xcom_pull(task_ids='filter_values', key='load_date') }}.parquet</b> успешно положен в S3.</p>
        <p>Путь: <code>s3://dev/api/artemw9/kz_{{ ti.xcom_pull(task_ids='filter_values', key='load_date') }}.parquet</code></p>
        <p>Кол-во строк: {{ ti.xcom_pull(task_ids='filter_values', key='total_rows') }}</p>
    """,
    conn_id="artemw9_mail",
    dag=dag,
)

send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="artemw9_tg",
    chat_id="{{ var.value.Artemw9_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "✅ Погода за {{ ti.xcom_pull(task_ids='filter_values', key='load_date') }} "
        "загружена\n"
        "Файл: <code>kz_{{ ti.xcom_pull(task_ids='filter_values', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='filter_values', key='total_rows') }}</b>"
    ),
    dag=dag,
)
upload >> filter_task >> [send_email, send_message_telegram]