
import pendulum
import logging
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

"""
типа тз:
агрегировать по категориальным признакам,
агрегировать метрики по среднему,
добавить дату, count, time_min_max
"""

logger = logging.getLogger("airflow.task")

OWNER = 'alexxxxxxela'
DB = OWNER

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 1),
}

dag = DAG(
    dag_id="alexxxxxxela_baranov__S3_to_CH_earthquake",
    default_args=default_args,
    schedule="0 7 * * *",
    catchup=False,
    description="S3 earthquake to CH by alexxxxxxela",
    tags=["earthquake", "s3", "ch", "spark"],
)


sensor_earthquake_S3 = ExternalTaskSensor(
    task_id="sensor_earthquake_S3",
    external_dag_id=f"{OWNER}_baranov__API_to_S3_earthquake_v2",
    mode="reschedule",
    poke_interval=10,
    timeout=3600,
    # execution_delta = pendulum.duration(minutes=1)
)

s3_to_ch = SparkSubmitOperator(
    task_id=f"spark_s3_to_ch_{OWNER}",
    application="/opt/airflow/scripts/transform/transform__earthquake_alexxxxxxela.py",
    conn_id="spark_default",
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "DB": DB,
        "TABLE_NAME": "earthquake_events",
        "CLUSTER_NAME": "company_cluster",
        "S3_PATH_EARTHQUAKE": 's3a://dev/alexxxxxxela/earthquake/',
        "PYTHONPATH": "/opt/airflow/plugins:/opt/airflow/scripts",
        "EXECUTION_DATE": "{{ logical_date }}",
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

sensor_earthquake_S3 >> s3_to_ch