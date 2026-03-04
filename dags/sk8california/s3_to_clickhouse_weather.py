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

# --- Логика подготовки списка файлов ---

def create_and_check_manifest_weather(**context):
    hook = S3Hook(aws_conn_id="minios3_conn")
    bucket = "dev"
    data_prefix = "sk8california/weather/daily/"
    manifest_key = "sk8california/weather_manifest.json"
    
    # Читаем переменную из Airflow (если нет — берем старую дату)
    last_check = Variable.get(
        "sk8california_last_s3_check_weather", 
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

    # Создаем манифест в S3 для истории
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
    
    # Пушим список файлов в XCom для Spark
    context["ti"].xcom_push(key="new_files_list", value=",".join(new_files))
    context["ti"].xcom_push(key="count_new_files", value=len(new_files))

def update_weather_checkpoint(**context):
    new_ts = datetime.now(timezone.utc).isoformat()
    Variable.set("sk8california_last_s3_check_weather", new_ts)
    logger.info(f"💾 Variable updated: {new_ts}")

# --- Определение DAG ---

with DAG(
    dag_id="sk8california__weather_s3_to_ch",
    default_args=default_args,
    schedule_interval="0 8 * * *", 
    catchup=False,
    tags=["weather", "S3", "Clickhouse"],
) as dag:

    # 1. Проверяем S3 и готовим список файлов
    prepare_manifest = PythonOperator(
        task_id="prepare_manifest",
        python_callable=create_and_check_manifest_weather,
    )

    # 2. Запускаем Spark со скриптом, который ты замержил
    spark_task = SparkSubmitOperator(
        task_id="spark_weather_transform",
        application="/opt/airflow/scripts/transform/transform_sk_weather.py",
        conn_id="spark_default",
        env_vars={
            "S3_PATH_DATA": "{{ ti.xcom_pull(task_ids='prepare_manifest', key='new_files_list') }}",
        },
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "s3_minio",
            "spark.hadoop.fs.s3a.secret.key": "4r5t6y7u",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse:clickhouse-jdbc:0.4.6"
    )

    # 3. Если всё прошло успешно — обновляем время последней проверки
    update_var = PythonOperator(
        task_id="update_checkpoint",
        python_callable=update_weather_checkpoint
    )

    # 4. Уведомление в Telegram
    notify = TelegramOperator(
        task_id="notify",
        telegram_conn_id="sk_telegram",
        chat_id="{{ var.value.SK_TELEGRAM_CHAT_ID }}",
        text="🌤 *Weather ETL Success!* \nОбработано новых файлов: {{ ti.xcom_pull(task_ids='prepare_manifest', key='count_new_files') }}"
    )

    # Очередность задач
    prepare_manifest >> spark_task >> update_var >> notify