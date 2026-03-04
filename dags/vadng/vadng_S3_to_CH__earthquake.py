import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.db import provide_session
from airflow.models import DagRun
from airflow.utils.dates import days_ago


default_args = {"owner": "vadng", "start_date": days_ago(1), "retries": 1}

dag = DAG(
    dag_id="vadng__S3_to_CH__earthquake",
    default_args=default_args,
    schedule_interval="00 6 * * *",
    description="S3 to Clickhouse earthquakes",
    catchup=False,
    tags=["spark", "transform"],
)

s3_to_ch = SparkSubmitOperator(
    task_id="spark_s3_to_ch",
    application="/opt/airflow/scripts/transform/transform__earthquake_vadng.py",
    conn_id="spark_default",
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/vadng",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "TABLE_NAME": "earthquakes",
        "S3_PATH_EARTHQUAKE": f's3a://{os.getenv("MINIO_DEV_BUCKET_NAME")}/vadng/API_to_S3/',
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

s3_to_ch
