from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import os


default_args = {
    "owner": "snccnmnb",
    "start_date": days_ago(1),
    "retries": 2,
}


dag = DAG(
    dag_id="snccnmb_users",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    catchup=False,
    description="S3 users to CH",
    tags=["users", "telegram", "bootcamp"],
)

dag.doc_md = __doc__


run_etl = BashOperator(
      task_id="run_etl",
      bash_command="python /opt/airflow/scripts/transform/transform_users_snccnmb.py",
      env={
            "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
            "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
            "CLICKHOUSE_HOST": "clickhouse01",
            "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/snccnmb",
            "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
            "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
            "PYTHONPATH": "/opt/airflow/plugins:/opt/airflow/scripts"
      },
      dag=dag,
)

run_etl
