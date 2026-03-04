import os
import json
import logging
from datetime import timedelta, datetime, timezone

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "sk8california",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def create_and_check_manifest(**context):
    hook = S3Hook(aws_conn_id="sk8california_minio")
    bucket = "dev"
    data_prefix = "sk8california/earthquakes/daily/"
    manifest_key = "sk8california/manifest.json"
    
    last_check = Variable.get(
        "sk8california_last_s3_check_earthquake", 
        default_var="2020-01-01T00:00:00"
    )
    last_check_dt = datetime.fromisoformat(last_check).replace(tzinfo=timezone.utc)
    
    logger.info(f"🔎 Scanning: {data_prefix} since {last_check_dt}")
    keys = hook.list_keys(bucket_name=bucket, prefix=data_prefix)
    
    if not keys:
        raise AirflowSkipException(f"No files found in {data_prefix}")

    new_files = []
    for key in keys:
        if key.endswith('/') or key == manifest_key:
            continue
            
        obj = hook.get_key(key, bucket_name=bucket)
        if obj.last_modified > last_check_dt:
            new_files.append(f"s3a://{bucket}/{key.lstrip('/')}")

    if not new_files:
        logger.info("🤷 No new files since last check.")
        raise AirflowSkipException("No new files to process.")

    manifest_content = {
        "execution_date": context['ds'],
        "files_count": len(new_files),
        "files": new_files
    }
    
    hook.load_string(
        string_data=json.dumps(manifest_content, indent=4),
        key=manifest_key,
        bucket_name=bucket,
        replace=True
    )
    logger.info(f"✅ Manifest updated: s3://{bucket}/{manifest_key}")

    context["ti"].xcom_push(key="new_files_list", value=",".join(new_files))
    context["ti"].xcom_push(key="count_new_files", value=len(new_files))

def update_checkpoint(**context):
    new_ts = datetime.now(timezone.utc).isoformat()
    Variable.set("sk8california_last_s3_check_earthquake", new_ts)
    logger.info(f"💾 Variable updated: {new_ts}")

with DAG(
    dag_id="sk8california__S3_to_CH",
    default_args=default_args,
    schedule_interval="0 7 * * *", 
    catchup=False,
    tags=["earthquake", "S3", "Clickhouse"],
) as dag:

    prepare_manifest = PythonOperator(
        task_id="prepare_manifest",
        python_callable=create_and_check_manifest,
    )

    spark_task = SparkSubmitOperator(
        task_id="spark_s3_to_ch",
        application="/opt/airflow/scripts/transform/transform_sk_earthquakes.py",
        conn_id="spark_default",
        env_vars={
            "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/sk8california", 
            "CLICKHOUSE_HOST": "clickhouse01",
            "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
            "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
            "TABLE_NAME": "sk8california.earthquakes", 
            "S3_PATH_REGIONS": "s3a://dev/jdbc/regions/",
            "S3_PATH_DATA": "{{ ti.xcom_pull(task_ids='prepare_manifest', key='new_files_list') }}",
            "PYTHONPATH": "/opt/airflow/plugins:/opt/airflow/scripts",
        },
        conf={
            "spark.executor.instances": "1",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "1",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.clickhouse:clickhouse-jdbc:0.4.6" 
        )
    )

    update_var = PythonOperator(
        task_id="update_checkpoint",
        python_callable=update_checkpoint
    )

    notify = TelegramOperator(
        task_id="notify",
        telegram_conn_id="sk_telegram",
        chat_id="{{ var.value.SK_TELEGRAM_CHAT_ID }}",
        text="🚀 *Success!* Manifest updated and Spark loaded {{ ti.xcom_pull(task_ids='prepare_manifest', key='count_new_files') }} files."
    )

    prepare_manifest >> spark_task >> update_var >> notify