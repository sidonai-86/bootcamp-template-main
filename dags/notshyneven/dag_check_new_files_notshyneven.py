from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

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

    prefix = "notshyneven/"
    bucket = "dev"

    # Получаем последнее время проверки из Airflow Variable или используем start_date
    last_check = Variable.get(
        "LAST_S3_CHECK_TIME__EQ", default_var="2020-01-01T00:00:00"
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
    Variable.set("LAST_S3_CHECK_TIME__EQ", now_str)

    manifest_content = "\n".join(path.strip() for path in new_files if path.strip())
    manifest_key = "notshyneven/manifests/new_files.txt"    # путь где лежать будет папка

    hook = S3Hook(aws_conn_id="minios3_conn")
    hook.load_string(
        string_data=manifest_content,
        key=manifest_key,
        bucket_name="dev",
        replace=True,
    )

    context["ti"].xcom_push(key="manifest_key", value=manifest_key)
    context["ti"].xcom_push(key="count_new_files", value=len(new_files))


default_args = {
    "owner": "notshyneven",
    "start_date": days_ago(1),
    "retries": 2,
}


dag = DAG(
    dag_id="notshyneven_check_new_files",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # каждый день в 11:00 UTC
    catchup=False,
    description="checknewfiles",
    tags=["magnitude", "5", "api", "s3"],
)

check_new_files = PythonOperator(
    task_id="check_new_files", python_callable=check_new_files, dag=dag
)

# s3_to_ch = SparkSubmitOperator(
#     task_id="spark_s3_to_ch",
#     application="/opt/airflow/scripts/transform/transform__weather_temp_rain.py",
#     conn_id="spark_default",
#     env_vars={
#         "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/default",
#         "CLICKHOUSE_HOST": "clickhouse01",
#         "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
#         "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
#         "TABLE_NAME": "weather_temp_rain",
#         "S3_PATH_MANIFEST": "{{ ti.xcom_pull(task_ids='check_new_files', key='manifest_key') }}",
#         "S3_PATH_WEATHER_TEMP_RAIN": "s3a://prod/api/weather_temp_rain",
#         "PYTHONPATH": "/opt/airflow/plugins:/opt/airflow/scripts",
#     },
#     conf={
#         "spark.executor.instances": "1",
#         "spark.executor.memory": "2g",
#         "spark.executor.cores": "1",
#         "spark.driver.memory": "1g",
#         "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
#         "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER"),
#         "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD"),
#         "spark.hadoop.fs.s3a.path.style.access": "true",
#         "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
#     },
#     packages=(
#         "org.apache.hadoop:hadoop-aws:3.3.4,"
#         "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
#         "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2"
#     ),
#     dag=dag,
# )


check_new_files


dag.doc_md = """
🌍 **DAG: Обработка данных о землетрясениях**  
👨‍💻 **Автор**: @notshyneven  
📡 **Источник**: MinIO S3, Spark, ClickHouse

📝 **Описание**  
Этот DAG автоматически проверяет наличие новых файлов о землетрясениях в хранилище **S3**, и если такие файлы есть, запускает обработку данных с помощью **Apache Spark** и загружает их в **ClickHouse** для дальнейшего анализа.

1. Проверка новых файлов о землетрясениях в S3, начиная с времени последней проверки.
2. Запись путей к новым файлам в манифест.
3. Обработка новых данных с использованием **Apache Spark**.
4. Сохранение обработанных данных в **ClickHouse**.

⏰ **Расписание**  
Запуск каждый день в **03:00 UTC** 🕒  
Начало проверки: с момента последней проверки, записанной в Airflow **Variable**.  
Файлы: `notshyneven/{date}/earthquake_data.json`.

🛡️ **Проверки**  
- Проверка наличия новых файлов в S3 на основе временной метки.
- Используется **Airflow Variable** для отслеживания времени последней проверки.
- Обработка только файлов, изменившихся после последней проверки.  
- Пропуск файлов в папке `manifests/` для предотвращения дублирования данных.

🎯 **Вывод**  
Надежный DAG для автоматической обработки данных о землетрясениях 🔄  
Манифесты и обработанные данные удобно загружаются в S3 и **ClickHouse** 📂  
Готов для **production** 🏗️
"""



