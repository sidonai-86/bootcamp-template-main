"""
# DAG «Moscow Weather (temp, rain)»

## Получаем данные из API Open-Meteo
    API Docs: https://open-meteo.com/en/docs

- Данные загружаются инкрементально один раз в день за вчера в 5:00
- Погода: Москва
- Чтобы перезагрузить ретро данные -> Clear Task
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
from datetime import datetime



def api_download(**context):
   
    load_date = context["ds"]
    ti = context["ti"]

    update_at = datetime.now()


    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": 55.7522,
            "longitude": 37.6156,
            "hourly": ["temperature_2m", "rain"],
            "start_date": load_date,
            "end_date": load_date,
        },
        timeout=60,
    )

    response.raise_for_status()
    data = response.json()
   
    filename = f"vvv_Nika/API_to_S3/moscow_{load_date}.parquet"

    df = pd.json_normalize(data)

    # почасовая нормализация
    hourly = data.get("hourly", {})
    df = pd.DataFrame({
        "time": hourly.get("time", []),
        "temperature_2m": hourly.get("temperature_2m", []),
        "rain": hourly.get("rain", []),
    })

    #Добавлено!
    df["update_at"] = update_at
    df["load_date"] = load_date
    

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

    print(f"==== Данные по погоде загружены за {load_date}")
    print(f"==== Кол-во строк {total_rows}")

    ti.xcom_push(key="load_date", value=load_date)
    ti.xcom_push(key="total_rows", value=total_rows)


default_args = {
    "owner": "vvv_Nika",
    "start_date": days_ago(10),
    "retries": 3,
}

dag = DAG(
    dag_id="vvv_Nika__API_to_S3_test",
    default_args=default_args,
    schedule_interval="0 5 * * *",  # каждый день в 05:00 UTC
    catchup=True,
    description="description_test",
    tags=["weather", "temp", "rain", "api", "s3"],
)

dag.doc_md = __doc__

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)

# https://api.telegram.org/bot<YourBOTToken>/getUpdates
send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="nika_tg",
    chat_id="{{ var.value.NIKA_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "✅ Погода за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружена\n"
        "Файл: <code>moscow_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)

upload >> send_message_telegram