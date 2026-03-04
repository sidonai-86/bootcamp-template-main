from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.db import provide_session
from airflow.models import DagRun
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


import os
import boto3
from datetime import datetime, timezone


def check_new_files(**context):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )

    prefix = "iadorokhin/"
    bucket = "dev"

    # Получаем последнее время проверки из Airflow Variable или используем start_date
    last_check = Variable.get(
        "LAST_S3_CHECK_TIME__IADOROKHIN", default_var="2020-01-01T00:00:00"
    )
    last_check = datetime.fromisoformat(last_check).replace(tzinfo=timezone.utc)

    # Получаем список всех файлов
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in response:
        raise AirflowSkipException("Нет файлов в S3.")

    new_files = []
    for obj in response["Contents"]:
        key = obj["Key"]
        if "manifests/" in key:
            continue  # Пропускаем файлы в папке manifests
        if obj["LastModified"] > last_check:
            print(f"LOG === new file: {key} @ {obj['LastModified']}")
            new_files.append(key)

    print(f"LOG === new_files {new_files}")
    if not new_files:
        raise AirflowSkipException("Нет новых файлов.")

    # Обновляем Variable с текущим временем
    now_str = datetime.now(timezone.utc).isoformat()
    print(f"LOG === update Variable {now_str}")
    Variable.set("LAST_S3_CHECK_TIME__IADOROKHIN", now_str)

    manifest_content = "\n".join(path.strip() for path in new_files if path.strip())
    manifest_key = "iadorokhin/manifests/new_files.txt"

    hook = S3Hook(aws_conn_id="minios3_conn")
    hook.load_string(
        string_data=manifest_content,
        key=manifest_key,
        bucket_name="dev",
        replace=True,
    )

    context["ti"].xcom_push(key="manifest_key", value=manifest_key)
    context["ti"].xcom_push(key="count_new_files", value=len(new_files))

default_args = {"owner": "iadorokhin", "start_date": days_ago(1), "retries": 1}

dag = DAG(
    dag_id="iadorokhin__S3_to_CH__earthquake",
    default_args=default_args,
    schedule_interval="0 11 * * *",
    description="Spark Submit Inc",
    catchup=False,
    tags=["spark", "iadorokhin"],
)

check_new_files = PythonOperator(
    task_id="check_new_files", python_callable=check_new_files, dag=dag
)

s3_to_ch = SparkSubmitOperator(
    task_id="spark_s3_to_ch",
    application="/opt/airflow/scripts/transform/transform__earthquake_iadorokhin.py",
    conn_id="spark_default",
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/default",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "TABLE_NAME": "earthquakes",
        "S3_PATH_EARTHQUAKE": f's3a://dev/iadorokhin/',
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

check_new_files >> s3_to_ch 
