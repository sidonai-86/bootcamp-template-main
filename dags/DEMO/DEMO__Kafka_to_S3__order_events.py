import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago


default_args = {"owner": "loader", "start_date": days_ago(1), "retries": 1}

dag = DAG(
    dag_id="DEMO__Kafka_to_S3__order_events",
    default_args=default_args,
    schedule_interval="@once",
    description="Spark Submit",
    catchup=False,
    tags=["spark", "streaming"],
)


stream_kafka_to_s3 = SparkSubmitOperator(
    task_id="spark_stream_kafka_to_s3",
    application="/opt/airflow/scripts/load/load__order_events.py",  # путь до spark-скрипта
    conn_id="spark_default",
    env_vars={
        "KAFKA_TOPIC": "source.public.order_events",
        "KAFKA_BOOTSTRAP": "kafka:29092",
        "S3_PATH": f's3a://{os.getenv("MINIO_PROD_BUCKET_NAME")}/stream/order_events/',
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
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    ),
    dag=dag,
)

stream_kafka_to_s3
