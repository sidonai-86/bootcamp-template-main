"""
# DAG avpalatov_GP_to_CH__shops_tz «st_id, shop_name, tz_code»

## Загрузка из GP в Clickhouse данных по магазинам с указанием часового пояса

## GP -> CH
- Витрина на CH каждый день полностью перезаписывается в 10 часов по Москве
"""

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

import os
from datetime import datetime, timezone


default_args = {
    "owner": "avpalatov",
    "start_date": days_ago(1),
    "retries": 2,
}


dag = DAG(
    dag_id="avpalatov_GP_to_CH__shops_tz",
    default_args=default_args,
    schedule_interval="0 7 * * *",  # каждый день в 07:00 UTC, 10 часов по Москве
    catchup=False,
    description="GP to CH shops tz",
    tags=["shops", "GP", "ClickHouse", "Spark"]
)

dag.doc_md = __doc__

gp_to_ch = SparkSubmitOperator(
    task_id="spark_gp_to_ch",
    application="/opt/airflow/scripts/transform/transform_shops_tz_avpalatov.py",
    conn_id="spark_default",
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/avpalatov",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "CLICKHOUSE_TABLE_NAME": "st_timezone",
        "CLICKHOUSE_CLUSTER_NAME": "company_cluster",
        "CLICKHOUSE_DATABASE":"avpalatov",
        "PG_JDBC_URL": "jdbc:postgresql://postgres_source:5432/source",
        "PG_USER": os.getenv("POSTGRES_USER"),
        "PG_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "PG_TABLE_NAME_SHOPS": "public.shops",
        "PG_TABLE_NAME_SHOPS_TZ": "public.shop_timezone",
    },
    conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "2g",
        "spark.executor.cores": "1",
        "spark.driver.memory": "1g",
    },
    packages=(
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
        "org.postgresql:postgresql:42.7.1"
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
            "🌤️ GP -> SHOPS_TZ "
            "📥 Данные в Clickhouse загружены"
        ),
        dag=dag,
    )


gp_to_ch >> notifications