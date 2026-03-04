import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'goggle_mogle',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='goggle_mogle_kafka_to_s3',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    kafka_to_s3 = SparkSubmitOperator(
        task_id="spark_kafka_to_s3_batch",
        application="/opt/airflow/scripts/transform/transform__kafka_to_s3_goggle_mogle.py",
        conn_id="spark_default",
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
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
        )
    )

    kafka_to_s3