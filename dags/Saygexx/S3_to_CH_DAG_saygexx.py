"""
DAG: S3 (prod) → ClickHouse (saygexx)

Тип пайплайна: Incremental ingestion через manifest.

Архитектурные принципы:
- Инкремент определяется по LastModified в S3.
- Состояние пайплайна хранится в Airflow Variable.
- Variable обновляется только после успешной загрузки Spark.
- Данные пишутся в Distributed таблицу.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

import boto3
import os
from datetime import datetime, timezone


# ------------------------------
# Конфигурация источника данных
# ------------------------------

BUCKET = "prod"                    # Bucket, где лежат parquet-файлы
PREFIX = "weather/hourly/"         # Префикс (папка)
VARIABLE_NAME = "LAST_S3_CHECK_TIME__WEATHER"
# Variable хранит timestamp последней успешной загрузки.
# Это state нашего пайплайна.


# ------------------------------
# 1️⃣ Проверка новых файлов
# ------------------------------
def check_new_weather_files(**context):
    """
    Логика:
    1. Получаем timestamp последней загрузки из Variable.
    2. Читаем список объектов в S3.
    3. Отбираем parquet-файлы новее last_check.
    4. Формируем manifest — список файлов для Spark.
    """

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )

    # Если Variable ещё не существует — используем старую дату
    last_check_str = Variable.get(
        VARIABLE_NAME,
        default_var="2020-01-01T00:00:00",
    )
    last_check = datetime.fromisoformat(last_check_str).replace(tzinfo=timezone.utc)

    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)

    if "Contents" not in response:
        # Корректная остановка DAG, если файлов нет
        raise AirflowSkipException("Нет файлов в S3.")

    new_files = []

    for obj in response["Contents"]:
        # Инкрементальная логика:
        # берём только parquet и только новее last_check
        if obj["Key"].endswith(".parquet") and obj["LastModified"] > last_check:
            new_files.append(f"s3a://{BUCKET}/{obj['Key']}")

    if not new_files:
        # Нет новых данных — DAG завершится со статусом skipped
        raise AirflowSkipException("Нет новых файлов.")

    # Manifest — механизм контроля.
    # Spark загрузит только те файлы, которые мы явно ему передали.
    manifest_content = "\n".join(new_files)
    manifest_key = f"{PREFIX}manifests/new_files.txt"

    hook = S3Hook(aws_conn_id="minios3_conn")
    hook.load_string(
        string_data=manifest_content,
        key=manifest_key,
        bucket_name=BUCKET,
        replace=True,
    )

    # Передаём путь к manifest в Spark через XCom
    context["ti"].xcom_push(key="manifest_key", value=manifest_key)
    context["ti"].xcom_push(key="count_new_files", value=len(new_files))


# ------------------------------
# 2️⃣ Обновление state
# ------------------------------
def update_last_check_time():
    """
    Обновляем Variable ТОЛЬКО после успешного Spark.
    Это гарантирует отсутствие потери данных при падении Spark.
    """
    now_str = datetime.now(timezone.utc).isoformat()
    Variable.set(VARIABLE_NAME, now_str)


default_args = {
    "owner": "saygexx",
    "start_date": days_ago(1),
    "retries": 2,
}


with DAG(
    dag_id="S3_to_CH_weather_incremental_saygexx",
    default_args=default_args,
    schedule_interval="0 11 * * *",
    catchup=False,  # т.к. логика не зависит от execution_date
    tags=["weather", "snow", "incremental", "spark"],
) as dag:

    # 1. Проверка S3
    check_files = PythonOperator(
        task_id="check_new_weather_files",
        python_callable=check_new_weather_files,
    )

    # 2. Spark-загрузка в Distributed таблицу
    s3_to_ch = SparkSubmitOperator(
        task_id="spark_s3_to_ch",
        application="/opt/airflow/scripts/transform/s3_to_clickhouse_weather_saygexx.py",
        conn_id="spark_default",
        env_vars={
            # Подключение к базе saygexx
            "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/saygexx",
            "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
            "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
            # Пишем в Distributed таблицу
            "TABLE_NAME": "weather_actual_distributed",
            "S3_PATH_MANIFEST": "{{ ti.xcom_pull(task_ids='check_new_weather_files', key='manifest_key') }}",
        },
        conf={
            # Настройки доступа Spark к MinIO
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.clickhouse:clickhouse-jdbc:0.4.6"
        ),
    )

    # 3. Обновление state после успешного Spark
    update_variable = PythonOperator(
        task_id="update_last_check_time",
        python_callable=update_last_check_time,
    )

    # 4. Уведомление об успехе
    send_success_tg = TelegramOperator(
        task_id="send_success_telegram",
        telegram_conn_id="saygexx_tg",
        chat_id="{{ var.value.saygexx_chat_id }}",
        text="❄️ Weather данные успешно загружены в ClickHouse",
    )

    # 5. Алерт при падении Spark
    send_failure_tg = TelegramOperator(
        task_id="send_failure_telegram",
        telegram_conn_id="saygexx_tg",
        chat_id="{{ var.value.saygexx_chat_id }}",
        text="❌ Weather DAG упал. Проверь логи.",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    check_files >> s3_to_ch >> update_variable >> send_success_tg
    s3_to_ch >> send_failure_tg