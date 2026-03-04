from __future__ import annotations

import io
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

USER_NAME = "tel_grigorii_spb"
MIN_MAGNITUDE = "4.5"
S3_BUCKET = "dev"
AWS_CONN_ID = "minios3_conn"
SMTP_CONN_ID = "smtp_halltape_yandex"


@dag(
    dag_id=f"{USER_NAME}__API_to_S3__earthquakes_1",
    start_date=days_ago(7),
    schedule="0 10 * * *",
    catchup=True,
    default_args={"owner": USER_NAME, "retries": 2},
    tags=["earthquakes", "disasters", "api", "s3"],
    description="API earthquakes to S3",
)
def earthquakes_api_to_s3():
    @task(task_id="upload")
    def upload_task(ds: str, **context):
        # ds приходит как "YYYY-MM-DD"
      
        load_date = datetime.strptime(ds, "%Y-%m-%d")
        end_date = load_date + timedelta(days=1)

        # для API лучше строки
        starttime = ds
        endtime = end_date.strftime("%Y-%m-%d")

        url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        response = requests.get(
            url,
            params={
                "format": "geojson",
                "starttime": starttime,
                "endtime": endtime,
                "minmagnitude": MIN_MAGNITUDE,
                "limit": "2000",
            },
            timeout=60,
        )
        print("HTTP status:", response.status_code)
        response.raise_for_status()

        data = response.json()
        result = [feature["properties"] for feature in data.get("features", [])]
        df = pd.json_normalize(result)

        # бизнес-дата события
        if not df.empty and "time" in df.columns:
            df["event_date"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.date
        else:
            df["event_date"] = []

        # дата выгрузки
        df["update_at"] = date.today()

        # безопасное имя файла
        load_date_str = ds
        filename = f"{USER_NAME}/earthquakes_{load_date_str}.parquet"

        # грузим в S3 только если есть строки
        if len(df) > 0:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            hook.load_bytes(
                bytes_data=buffer.read(),
                key=filename,
                bucket_name=S3_BUCKET,
                replace=True,
            )

        total_rows = int(len(df))
        print(f"==== Данные по землетрясениям загружены за {load_date_str}")
        print(f"==== Кол-во строк {total_rows}")

        # вернём метаданные — это попадёт в XCom как return_value
        return {
            "load_date": load_date_str,
            "total_rows": total_rows,
            "min_magnitude": MIN_MAGNITUDE,
            "s3_bucket": S3_BUCKET,
            "s3_key": filename,
        }

    meta = upload_task()

    send_email = EmailOperator(
        task_id="send_email",
        to="gri.zatulo@yandex.ru",
        subject="Earthquakes parquet загружен за {{ ti.xcom_pull(task_ids='upload')['load_date'] }}",
        html_content="""
            <p>Файл <b>earthquakes_{{ ti.xcom_pull(task_ids='upload')['load_date'] }}.parquet</b> успешно положен в S3.</p>
            <p>Путь: <code>s3://{{ ti.xcom_pull(task_ids='upload')['s3_bucket'] }}/{{ ti.xcom_pull(task_ids='upload')['s3_key'] }}</code></p>
            <p>Кол-во строк: {{ ti.xcom_pull(task_ids='upload')['total_rows'] }}</p>
            <p>Минимальная магнитуда: {{ ti.xcom_pull(task_ids='upload')['min_magnitude'] }}</p>
        """,
        conn_id=SMTP_CONN_ID,
    )

    # зависимости
    meta >> send_email


earthquakes_api_to_s3()