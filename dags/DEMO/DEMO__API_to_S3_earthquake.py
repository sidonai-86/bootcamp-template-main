import json
import requests
from datetime import timedelta
import logging
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from db_utils import S3MaxDateManager

logger = logging.getLogger("airflow.task")


default_args = {"owner": "loader", "start_date": days_ago(7)}

dag = DAG(
    dag_id="DEMO__API_to_S3_earthquake",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True,
    description="API earthquake to S3",
    tags=["earthquake", "s3", "airflow"],
)


def loading_date(starttime: str, endtime: str, manager: S3MaxDateManager):

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    for dt in pd.date_range(starttime, endtime, inclusive="left"):
        load_date = dt.strftime("%Y-%m-%d")
        params = {
            "format": "geojson",
            "starttime": load_date,
            "endtime": (dt + timedelta(days=1)).strftime("%Y-%m-%d"),
        }

        logger.info(f"📡 Загрузка данных за {load_date}")

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if len(data["features"]) == 0:
            logger.info(f"🔍 Нет событий за {load_date}")
            return

        filename = f"api/earthquake/events_{load_date}.json"

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_string(
            string_data=json.dumps(data), key=filename, bucket_name="prod", replace=True
        )

        manager.update_max_value(load_date)
        logger.info(f"✅ Данные за {load_date} загружены")


def fetch_and_upload(**context):
    table_name = "earthquake"
    manager = S3MaxDateManager(table_name, init_value=default_args.get("start_date"))

    starttime = context["execution_date"].strftime("%Y-%m-%d")
    endtime = (context["execution_date"] + timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        loading_date(starttime, endtime, manager)
    except Exception as e:
        logger.error(f"❌ Ошибка запроса или загрузки: {e}", exc_info=True)


fetch_and_upload = PythonOperator(
    task_id="fetch_and_upload", python_callable=fetch_and_upload, dag=dag
)

fetch_and_upload
