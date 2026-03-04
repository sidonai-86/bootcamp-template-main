import os
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.python import PythonSensor
from airflow.utils.db import provide_session
from airflow.models import DagRun
from airflow.utils.dates import days_ago


default_args = {"owner": "fut3r", "start_date": days_ago(6), "retries": 1}

dag = DAG(
    dag_id="fut3r__S3_to_GP_books",
    default_args=default_args,
    schedule_interval="30 6 * * *",
    description="load to GP",
    catchup=True,
    tags=["spark", "transform"],
)

sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_api_upload",
        external_dag_id="fut3r__API_to_S3_books",
        external_task_id='upload',
        execution_delta=timedelta(hours=1, minutes=30),
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=600,                       
        timeout=700, 
        dag=dag                            
)
    


s3_to_gp = SparkSubmitOperator(
    task_id="spark_s3_to_gp",
    application="/opt/airflow/scripts/transform/transform_fut3r_books.py",
    conn_id="spark_default",
    env_vars={
            "GP_HOST": "master",
            "GP_PORT": "5432",
            "GP_DB": "postgres",
            "GP_USER": os.getenv("GREENPLUM_USER"),
            "GP_PASSWORD": os.getenv("GREENPLUM_PASSWORD")
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
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
        "org.postgresql:postgresql:42.5.0"
    ),
    dag=dag,
)

sensor_on_raw_layer >> s3_to_gp