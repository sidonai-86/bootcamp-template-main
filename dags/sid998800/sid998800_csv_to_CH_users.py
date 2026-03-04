"""
# DAG csv_to_CH_users («telegram_id, user_nickname, registration_date»)

## Загрузка списка пользователей в ClickHouse из csv

### Что делает DAG
- 🕓Ежедневно (в 04:00 UTC) перезаписывает таблицу tg_users в ClickHouse
- 🧛Параметры: author, title, description, url, urlToImage, publishedAt
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

import os


default_args = {
    "owner": "sid998800",
    "start_date": days_ago(2),
    "retries": 2,
}

dag = DAG(
    dag_id="sid998800__scv_to_ch_users",
    default_args=default_args,
    schedule_interval="0 4 * * *",  # каждый день в 04:00 UTC
    catchup=True,
    description="CSV(tg_users) to clickhouse",
    tags=["csv", "spark", "tg_users"],
)

dag.doc_md = __doc__


def extract_count(**context):
    try:
        with open("/opt/airflow/data/spark_count.txt", "r") as f:
            count = f.read().strip()
            print(f"Прочитано из файла: {count}")
            # ЯВНО отправляем в XCom
            return count
    except FileNotFoundError:
        print("❌ Файл с count не найден")
        return 0
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return 0


spark_csv_to_ch = SparkSubmitOperator(
    task_id="sid998800_spark_csv_to_ch",
    application="/opt/airflow/scripts/transform/transform_sid998800_users_spark.py",
    conn_id="spark_default",
    do_xcom_push=True,
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/sid998800",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
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
        "spark.TABLE_NAME": "tg_users",
        "spark.CLUSTER_NAME": "company_cluster",
        "spark.DATABASE": "sid998800",
    },
    packages=(
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2"
    ),
    dag=dag,
)

extract_count_task = PythonOperator(
    task_id="extract_count_task",  # task_id для xcom_pull
    python_callable=extract_count,
    dag=dag,
)

send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="sid_tg",
    chat_id="{{ var.value.sid998800_tg_chat_id }}",  # ..."chat":{"id":CHAT_ID,"firs...
    text=(
        "📋 CSV -> ClickHouse: "
        "👌 Успешно загружено {{ ti.xcom_pull(task_ids='extract_count_task') }} строк в Clickhouse"
    ),
    dag=dag,
)

spark_csv_to_ch >> extract_count_task >> send_message_telegram
