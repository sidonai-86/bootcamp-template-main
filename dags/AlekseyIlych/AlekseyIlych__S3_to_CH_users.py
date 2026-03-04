from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

import os
import boto3
from datetime import datetime, timezone


default_args = {
    "owner": "AlekseyIlych",
    "start_date": days_ago(1),
    "retries": 2,
}


dag = DAG(
    dag_id="AlekseyIlych__S3_to_CH_users",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=False,
    description="users",
    tags=["users", "CSV", "ClickHouse", "Spark"]
)

csv_to_ch = SparkSubmitOperator(
    task_id="spark_csv_to_ch",
    application="/opt/airflow/scripts/transform/transform_users_AlekseyIlych.py",
    conn_id="spark_default",
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/AlekseyIlych",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "TABLE_NAME": "users",
        "S3_PATH": "s3a://dev/AlekseyIlych/users",
        "PYTHONPATH": "/opt/airflow/plugins:/opt/airflow/scripts",
    },
    conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "2g",
        "spark.executor.cores": "1",
        "spark.driver.memory": "1g",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    },
    packages=(
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2"
    ),
    dag=dag,
)

send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="AlekseyIlych_tg",
    chat_id="{{ var.value.ALEKSEYILYCH_TELEGRAM_CHAT_ID }}",
    text=(
        "csv_users -> CH\n"
        "Данные в Clickhouse загружены"
    ),
    dag=dag,
)

csv_to_ch >> send_message_telegram
