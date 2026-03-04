from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.utils.context import Context

from datetime import timedelta
import json
import requests


def get_weather_data(**context: Context):
    base_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 55.75,
        "longitude": 37.62,
        "current_weather": True,
    }

    response = requests.get(base_url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    logical_date = context["logical_date"].strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"api/weather_Moscow_{logical_date}.json"

    hook = S3Hook(aws_conn_id="minios3_conn")
    hook.load_string(
        string_data=json.dumps(data, ensure_ascii=False),
        key=filename,
        bucket_name="prod",
        replace=True,
    )

    print(f"uploaded to s3://prod/{filename}")


default_args = {
    "owner": "Usmanov_RR",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="usmanov_rr_api_to_s3",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    description="Загрузка текущей погоды в S3 (open-meteo)",
    tags=["витрина_погоды", "weather"],
) as dag:

    upload_weather_to_s3 = PythonOperator(
        task_id="upload_weather_to_s3",
        python_callable=get_weather_data,
        provide_context=True,
    )

    upload_weather_to_s3
