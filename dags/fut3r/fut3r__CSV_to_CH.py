import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.db import provide_session
from airflow.models import DagRun
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

'''
def upload_folder_to_s3(folder_path):
    hook = S3Hook('aws_conn_id="minios3_conn"')
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            full_path = os.path.join(folder_path, filename)
            print(f"Uploading {filename}...")
            hook.load_file(
                filename=full_path,
                key=f"fut3r/users/{filename}",
                bucket_name="dev",
                replace=True
            )
'''

default_args = {"owner": "fut3r", "start_date": days_ago(1), "retries": 1}

dag = DAG(
    dag_id="fut3r__csv_to_CH__",
    default_args=default_args,
    schedule="0 7 * * *",
    description="load to CH",
    catchup=False,
    tags=["spark", "transform", "csv"],
)

download_files = BashOperator(
        task_id="download_csv", 
        bash_command='echo "Я нахожусь в: $(pwd)" && \
curl -L "https://huggingface.co/datasets/halltape/users/resolve/main/google_users.csv" -o /tmp/google_users.csv && \
curl -L "https://huggingface.co/datasets/halltape/users/resolve/main/other_users.csv" -o /tmp/other_users.csv && \
curl -L "https://huggingface.co/datasets/halltape/users/resolve/main/yandex_users.csv" -o /tmp/yandex_users.csv',
        dag=dag
    )

'''
upload_to_s3 = PythonOperator(
    task_id="upload_files",
    python_callable=upload_folder_to_s3,
    op_kwargs={
        'folder_path': '/tmp'
    }
)
'''

spark_tranform = SparkSubmitOperator(
    task_id="spark_s3_to_ch",
    application="/opt/airflow/scripts/transform/transform__users_fut3r_csv.py",
    files='/tmp/google_users.csv,/tmp/other_users.csv,/tmp/yandex_users.csv',
    conn_id="spark_default",
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/fut3r",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "TABLE_NAME": "users",
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

download_files  >> spark_tranform