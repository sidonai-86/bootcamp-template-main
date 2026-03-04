from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import io
import requests
import pandas as pd
from datetime import datetime, date

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def convert_unix_date(convert_date: int) -> datetime:
    return datetime.fromtimestamp(convert_date / 1000)


def api_download(**context):
    ds = context["ds"]
    next_ds = context["next_ds"]
    url = context["base_url"]
    start_date = ds
    end_date = next_ds
    params = {
        "format": "geojson",
        "starttime": start_date,
        "endtime": end_date,
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    features = response.json().get("features")
    data = [feature.get("properties") for feature in features]
    df = pd.DataFrame(data)
    # Переводим время землетресения из UNIXtime
    df["business_date"] = df["time"].apply(convert_unix_date)
    df["update_at"] = date.today().strftime("%Y-%m-%d")
    filename = f"x4Luck/earthquake/earthquake_{ds}.parquet"
    if len(df) != 0:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)  # возвращаемся в начало буфера

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(), key=filename,
            bucket_name="dev", replace=True
        )
    print(f"--- Данные о землетресениях загружены за {ds} ---")
    print()
    print(f"--- Количесвто строк = {len(df)} ---")


default_args = {
    "owner": "x4luck",
    "start_date": days_ago(2),
    "retries": 2,
}

dag = DAG(
    dag_id="x4Luck_API_to_S3_earthquake",
    default_args=default_args,
    schedule_interval="0 10 * * *",  # каждый день в 10:00 UTC
    catchup=True,
    max_active_runs=1,
    description="API earthquake to S3",
    tags=["earthquake", "api", "s3"],
)

upload = PythonOperator(
    task_id="upload",
    python_callable=api_download,
    op_kwargs={"base_url": BASE_URL},
    dag=dag,
)
