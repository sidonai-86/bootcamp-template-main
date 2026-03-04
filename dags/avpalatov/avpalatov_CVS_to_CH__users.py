"""
# DAG avpalatov_CVS_to_CH__users «telegram_id, user_nickname, registration_date»

## Загрузка файлов с результатами опроса «Кто хочет на буткемп»

## CVS -> CH
- Витрина на CH каждый день полностью перезаписывается в 10 часов по Москве
"""

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
    "owner": "avpalatov",
    "start_date": days_ago(1),
    "retries": 2,
}


dag = DAG(
    dag_id="avpalatov_CVS_to_CH__users",
    default_args=default_args,
    schedule_interval="0 7 * * *",  # каждый день в 07:00 UTC, 10 часов по Москве
    catchup=False,
    description="CVS to CH users",
    tags=["users", "CVS", "ClickHouse", "Spark"]
)

dag.doc_md = __doc__


cvs_to_ch = SparkSubmitOperator(
    task_id="spark_cvs_to_ch",
    application="/opt/airflow/scripts/transform/transform__users_avpalatov.py",
    conn_id="spark_default",
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/avpalatov",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "TABLE_NAME": "users",
        "S3_PATH": "s3a://dev/avpalatov/users",
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

with TaskGroup(group_id="notifications", dag=dag) as notifications:
    # https://api.telegram.org/bot<YourBOTToken>/getUpdates
    send_message_telegram = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id="avpalatov_tg",
        chat_id="{{ var.value.AVPALATOV_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
        text=(
            "🌤️ SCVS -> Users "
            "📥 Данные в Clickhouse загружены"
        ),
        dag=dag,
    )


cvs_to_ch >> notifications
