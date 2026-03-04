import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'loader',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id="DEMO__PG_to_S3__app_installs",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    description="Spark Submit Inc",
    catchup=False,
    tags=['spark', 'batch']
)


jdbc_to_s3 = SparkSubmitOperator(
    task_id='spark_jdbc_to_s3',
    application='/opt/airflow/scripts/load/load__app_installs.py',
    conn_id='spark_default',
    env_vars={
        'POSTGRES_JDBC_URL': 'jdbc:postgresql://postgres_source:5432/source',
        'POSTGRES_USER': os.getenv('POSTGRES_USER'),
        'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD'),
        'TABLE_NAME': 'public.app_installs',
        'S3_APP_INSTALLS_PATH': f's3a://{os.getenv("MINIO_PROD_BUCKET_NAME")}/jdbc/app_installs/',
        'PYTHONPATH': '/opt/airflow/plugins:/opt/airflow/scripts'
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
        "spark.hadoop.fs.s3a.connection.timeout": "60000",
    },
    packages=(
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.5.0"
    ),
    dag=dag
)

jdbc_to_s3
