"""
# DAG «Manuas Weather (temp, rain)»

## Получаем данные из API Open-Meteo
    API Docs: https://open-meteo.com/en/docs

- Данные загружаются инкрементально один раз в день за вчера в 9:00
- Погода: Мануас
- Чтобы перезагрузить ретро данные -> Clear Task
"""



from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import io
import pandas as pd
import requests


def api_download(**context):
    
    ti = context["ti"]
    
    LOAD_DATE = context['ds']
    api_url = "https://api.open-meteo.com/v1/forecast"
    LATITUDE = -2.00
    LONGITUDE = -54.081
   

    response = requests.get(api_url, 
    params ={
        'latitude': LATITUDE,
        'longitude': LONGITUDE,
        'hourly': ['temperature_2m', 'rain'],
        'start_date': LOAD_DATE,
        'end_date': LOAD_DATE,
    }
    )
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data['hourly'])
    
    total_rows = len(df)

    print(f"==== Данные по погоде загружены за {LOAD_DATE}")
    print(f"==== Кол-во строк {total_rows}")

    filename = f"sava_n/api_weather/{LOAD_DATE}.parquet"


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
        
        
    # total_rows = len(df)

    # print(f"==== Данные по погоде загружены за {load_date}")
    # print(f"==== Кол-во строк {total_rows}")

    ti.xcom_push(key="load_date", value=LOAD_DATE)
    ti.xcom_push(key="total_rows", value=total_rows)

default_args = {
    "owner": "sava_nn",
    "start_date": days_ago(15),
    "retries": 3,
}


dag = DAG(
    dag_id="sava_n_API_to_S3__weather",
    default_args=default_args,
    schedule_interval="0 9 * * *",  # каждый день в 09:00 UTC
    catchup=True,
    description="weather_api",
    tags=["weather", "temp", "rain", "api", "s3"],
)

dag.doc_md = __doc__

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)



send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="sav_telegram",
    chat_id="{{ var.value.SAV_TELEGRAM }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "✅ Погода за  {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружена\n"
        "Файл: <code>manuas_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)

upload >> send_message_telegram

