"""
# DAG «Earthquakes 🌍»

## S3 -> CH

- запуск каждый день в 04:00
- проверяет даг Denis_Chinese_API_to_S3__eq на успешный запуск
- в случае успеха берет новые файлы из S3, трансформирует и закидывает в ClickHouse, 
- в случае, если успешных запусков нет - ждет и повторно опрашивает 12 часов
"""

import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.db import provide_session
from airflow.models import DagRun
from airflow.utils.dates import days_ago


default_args = {"owner": "Denis_Chinese", "start_date": days_ago(1), "retries": 1}

dag = DAG(
    dag_id="Denis_Chinese__S3_to_CH__eq",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    description="Transform and load files from S3 to CH",
    catchup=False,
    tags=["spark", "transform", "earthquakes"],
)

dag.doc_md = __doc__

@provide_session
def check_pg_to_s3_done(session=None, **kwargs):
    dag_runs = (
        session.query(DagRun)
        .filter(DagRun.dag_id == "Denis_Chinese_API_to_S3__eq", DagRun.state == "success")
        .all()
    )
    return len(dag_runs) > 0


wait_for_API_to_S3_dag = PythonSensor(
    task_id="wait_for_API_to_S3_loader",
    python_callable=check_pg_to_s3_done,
    poke_interval=60,
    timeout=60 * 60 * 12,
    mode="poke",
    dag=dag,
)


s3_to_ch = SparkSubmitOperator(
    task_id="spark_s3_to_ch",
    application="/opt/airflow/scripts/transform/transform__earthquake_denis.py",
    conn_id="spark_default",
    do_xcom_push=True,
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/Denis_Chinese",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "TABLE_NAME": "earthquake",
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
    telegram_conn_id="denis_tg",
    chat_id="{{ var.value.DENIS_TELEGRAM_CHAT_ID }}",
    text=(
        "🌍 DAG «Earthquakes\n"
        "📥 S3 -> Clickhouse\n"
        "отработал без ошибок"
    )
)
wait_for_API_to_S3_dag >> s3_to_ch >> send_message_telegram