import pendulum 
import logging

from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.api.earthquake_api_to_s3 import earthquake_api_to_s3

logger = logging.getLogger("airflow.task")

OWNER = 'alexxxxxxela'

def get_config():
    # deserialize_json=True автоматически превратит JSON-строку в dict
    DEFAULT_CONFIG = {
        "strategy_name": "parquet",
        "base_path": f"dev/{OWNER}/earthquake/",
    }
    return Variable.get(
        f"ea_{OWNER}_config", default_var=DEFAULT_CONFIG, deserialize_json=True
    )

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 1),
}

dag = DAG(
    dag_id=f"{OWNER}_baranov__API_to_S3_earthquake_v2",
    default_args=default_args,
    schedule="0 7 * * *",
    catchup=False,
    description=f"API earthquake to S3 by {OWNER}",
    tags=["earthquake", "s3", "ch", "spark"],
)

def loading_date(**context):

    starttime = context["execution_date"].strftime("%Y-%m-%d")
    endtime = (context["execution_date"] + pendulum.duration(days=1)).strftime("%Y-%m-%d")

    config = get_config()

    earthquake_api_to_s3(starttime, endtime, config)



loading_date = PythonOperator(
    task_id="loading_date", python_callable=loading_date, dag=dag
)

loading_date
