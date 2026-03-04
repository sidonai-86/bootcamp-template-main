"""
# GitHub API - List public events
DAG для загрузки публичных событий GitHub в S3 хранилище
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException

import io
import pandas as pd
import requests
import logging

logger = logging.getLogger(__name__)


def api_download(**context) -> None:
    # Загрузка данных из GitHub API и сохранение в S3

    load_date = context["ds"]

    try:
        # Загрузка данных из API
        response = requests.get(
            "https://api.github.com/events",
            headers={"Accept": "application/vnd.github+json"},
            params={"per_page": 100, "page": 3},
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()

        if not data:
            logger.info(f"No data available for {load_date}")
            return

        df = pd.json_normalize(data)

        if len(df) == 0:
            logger.info(f"Empty DataFrame for {load_date}")
            return

        # Обработка данных
        df_no_nan = df.dropna(axis=1, how="any")

        if len(df_no_nan.columns) == 0:
            logger.warning(f"All columns contain NaN values for {load_date}")
            return

        df_no_nan.insert(0, "update_at", load_date)

        # Сохранение в буфер
        buffer = io.BytesIO()
        df_no_nan.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Загрузка в S3
        filename = (
            f"const_dobro/github_events/github_list_pub_events_{load_date}.parquet"
        )

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(), key=filename, bucket_name="dev", replace=True
        )

        logger.info(f"GitHub API data loaded for {load_date}")
        logger.info(f"Number of rows: {len(df)}")
        logger.info(f"Saved to: {filename}")

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise AirflowException(f"API request failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise AirflowException(f"Unexpected error: {str(e)}")


default_args = {
    "owner": "const_dobro",
    "start_date": days_ago(30),  # Конкретная дата вместо days_ago
    "retries": 2,
}

dag = DAG(
    dag_id="const_dobro_github_api_to_s3",  # Исправленное название
    default_args=default_args,
    schedule_interval="0 10 * * *",  # Каждый день в 10:00 UTC
    catchup=False,  # Без кетчапа по умолчанию
    description="GitHub API to S3 - Public events",
    tags=["github", "events", "api", "s3"],
    max_active_runs=1,
)

dag.doc_md = __doc__

upload = PythonOperator(
    task_id="download_github_events",
    python_callable=api_download,
    dag=dag,
    provide_context=True,
)

upload
