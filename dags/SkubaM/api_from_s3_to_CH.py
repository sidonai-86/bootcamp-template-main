"""
# DAG «Yandex Metrika: Visits / Users (Moscow, SPB)»
## S3 -> ClickHouse через Spark

- Ищет новые/изменённые parquet в S3 (MinIO) по LastModified > checkpoint
- Пишет manifest в S3
- Spark читает manifest и грузит данные в ClickHouse
- Checkpoint обновляется ТОЛЬКО после успешной загрузки (чтобы не терять файлы при падениях)
"""

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

import os
import boto3
from datetime import datetime, timezone


S3_BUCKET = "dev"
S3_PREFIX = "SkubaM/"
S3_MANIFEST_KEY = "SkubaM/manifests/yandex_visits_users_new_files.txt"

FILE_CONTAINS = "msc_spb_visits_history_"
FILE_EXT = ".parquet"

VAR_LAST_CHECK = "LAST_S3_CHECK_TIME__YANDEX_VISITS_USERS"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )


def build_manifest_incremental(**context):
    last_check_str = Variable.get(
        VAR_LAST_CHECK, default_var="2020-01-01T00:00:00+00:00"
    )
    last_check = datetime.fromisoformat(last_check_str)
    if last_check.tzinfo is None:
        last_check = last_check.replace(tzinfo=timezone.utc)

    s3 = _s3_client()

    new_files = []
    max_lm = None

    token = None
    while True:
        kwargs = {"Bucket": S3_BUCKET, "Prefix": S3_PREFIX}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            key = obj["Key"]

            if "/manifests/" in key or key.endswith("/"):
                continue

            if (FILE_CONTAINS not in key) or (not key.endswith(FILE_EXT)):
                continue

            lm = obj.get("LastModified")
            if not lm:
                continue
            if lm.tzinfo is None:
                lm = lm.replace(tzinfo=timezone.utc)

            if lm > last_check:
                new_files.append(key)
                if (max_lm is None) or (lm > max_lm):
                    max_lm = lm

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    if not new_files:
        raise AirflowSkipException("Нет новых/изменённых parquet-файлов")

    new_files = sorted(set(new_files))
    manifest_content = "\n".join(new_files)

    hook = S3Hook(aws_conn_id="minios3_conn")
    hook.load_string(
        string_data=manifest_content,
        key=S3_MANIFEST_KEY,
        bucket_name=S3_BUCKET,
        replace=True,
    )

    checkpoint_str = max_lm.astimezone(timezone.utc).isoformat() if max_lm else None

    ti = context["ti"]
    ti.xcom_push(key="manifest_key", value=S3_MANIFEST_KEY)
    ti.xcom_push(key="files_count", value=len(new_files))
    ti.xcom_push(key="checkpoint_time", value=checkpoint_str)

    print(f"Manifest: s3a://{S3_BUCKET}/{S3_MANIFEST_KEY}")
    print(f"Новых/изменённых файлов: {len(new_files)}")
    print(f"checkpoint_time: {checkpoint_str}")


def commit_checkpoint(**context):
    ti = context["ti"]
    checkpoint_str = ti.xcom_pull(
        task_ids="build_manifest_incremental", key="checkpoint_time"
    )
    if not checkpoint_str:
        raise AirflowSkipException("checkpoint_time пустой — не обновляем Variable")

    Variable.set(VAR_LAST_CHECK, checkpoint_str)
    print(f"Updated {VAR_LAST_CHECK} = {checkpoint_str}")


default_args = {
    "owner": "SkubaM",
    "start_date": days_ago(1),
    "retries": 2,
}

dag = DAG(
    dag_id="SkubaM_S3_to_CH_yandex_visits_users",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["yandex", "metrika", "s3", "spark", "clickhouse"],
)

build_manifest = PythonOperator(
    task_id="build_manifest_incremental",
    python_callable=build_manifest_incremental,
    dag=dag,
)

spark_load = SparkSubmitOperator(
    task_id="spark_s3_to_clickhouse",
    application="/opt/airflow/scripts/transform/transform_yandex_visits_users_to_ch_SkubaM.py",
    conn_id="spark_default",
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/SkubaM",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "CLICKHOUSE_DATABASE": "SkubaM",
        "CLICKHOUSE_CLUSTER": "company_cluster",
        "TABLE_NAME": "yandex_visits_users",
        "S3_BUCKET": S3_BUCKET,
        "S3_MANIFEST_KEY": "{{ ti.xcom_pull(task_ids='build_manifest_incremental', key='manifest_key') }}",
        "MINIO_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
        "MINIO_ACCESS_KEY": os.getenv("MINIO_ROOT_USER"),
        "MINIO_SECRET_KEY": os.getenv("MINIO_ROOT_PASSWORD"),
        "PYTHONPATH": "/opt/airflow/plugins:/opt/airflow/scripts",
    },
    conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "2g",
        "spark.executor.cores": "1",
        "spark.driver.memory": "1g",
        "spark.hadoop.fs.s3a.endpoint": os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    },
    packages=(
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "com.clickhouse:clickhouse-jdbc:0.6.5"
    ),
    dag=dag,
)

commit = PythonOperator(
    task_id="commit_checkpoint",
    python_callable=commit_checkpoint,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

build_manifest >> spark_load >> commit