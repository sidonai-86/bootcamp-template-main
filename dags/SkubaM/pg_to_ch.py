"""
Выгружает справочник из PG, преобразует через spark и загружает в ClickHouse.
"""

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

import os
from datetime import datetime, timezone


default_args = {
    "owner": "SkubaM",
    "start_date": days_ago(1),
    "retries": 2,
}

dag = DAG(
    dag_id="SkubaM_PG_to_CH_shops",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=False,
    tags=["shops", "pg", "spark", "clickhouse"],
)


spark_load = SparkSubmitOperator(
    task_id="spark_pg_to_clickhouse",
    application="/opt/airflow/scripts/transform/transform_pg_to_ch_SkubaM.py",
    conn_id="spark_default",
    env_vars={
        "PG_JDBC_URL": "jdbc:postgresql://postgres_source:5432/source",
        "PG_USER": os.getenv("POSTGRES_USER"),
        "PG_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "PG_SHOPS_TABLE": "public.shops",
        "PG_TZ_TABLE": "public.shop_timezone",

        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/SkubaM",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "CLICKHOUSE_DATABASE": "SkubaM",
        "CLICKHOUSE_CLUSTER": "company_cluster",
        "TABLE_NAME": "shops",
        "WRITE_MODE": "append",

        "PYTHONPATH": "/opt/airflow/plugins:/opt/airflow/scripts",
    },
    packages=(
        "org.postgresql:postgresql:42.5.0,"
        "com.clickhouse:clickhouse-jdbc:0.6.5"
    ),
    conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "2g",
        "spark.executor.cores": "1",
        "spark.driver.memory": "1g",
    },
    dag=dag,
)


spark_load