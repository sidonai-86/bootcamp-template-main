"""
# DAG «Moscow Weather (temp, rain)»

## Получаем данные из API Open-Meteo
    API Docs: https://open-meteo.com/en/docs

- Данные загружаются инкрементально один раз в день за вчера в 10:00
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


def api_download(**context):

    load_date = context["ds"]
    ti = context["ti"]

    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": 55.7522,
            "longitude": 37.6156,
            "hourly": ["temperature_2m", "rain"],
            "start_date": load_date,
            "end_date": load_date,
        },
    )

    response.raise_for_status()
    data = response.json()

    filename = f"api/weather_temp_rain/moscow_{load_date}.parquet"

    df = pd.json_normalize(data)

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
            bytes_data=buffer.read(), key=filename, bucket_name="prod", replace=True
        )

    total_rows = len(df)
    print(f"==== Данные по погоде загружены за {load_date}")
    print(f"==== Кол-во строк {total_rows}")

    ti.xcom_push(key="load_date", value=load_date)
    ti.xcom_push(key="total_rows", value=total_rows)


default_args = {
    "owner": "loader",
    "start_date": days_ago(30),
    "retries": 2,
}


dag = DAG(
    dag_id="DEMO__API_to_S3__weather_temp_rain",
    default_args=default_args,
    schedule_interval="0 10 * * *",  # каждый день в 10:00 UTC
    catchup=True,
    description="API weather to S3",
    tags=["weather", "temp", "rain", "api", "s3"],
)

dag.doc_md = __doc__

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)


send_email = EmailOperator(
    task_id="send_email",
    to="halltape@yandex.ru",
    subject="Weather parquet загружен за {{ ti.xcom_pull(task_ids='upload', key='load_date') }}",
    html_content="""
        <p>Файл <b>moscow_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</b> успешно положен в S3.</p>
        <p>Путь: <code>s3://dev/api/halltape_vindyukov/moscow_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code></p>
        <p>Кол-во строк: {{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</p>
    """,
    conn_id="smtp_halltape_yandex",
    dag=dag,
)


# https://api.telegram.org/bot<YourBOTToken>/getUpdates
send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="halltape_tg",
    chat_id="{{ var.value.HALLTAPE_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "✅ Погода за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружена\n"
        "Файл: <code>moscow_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
        "Кол-во строк: <b>{{ ti.xcom_pull(task_ids='upload', key='total_rows') }}</b>"
    ),
    dag=dag,
)


upload >> [send_email, send_message_telegram]
