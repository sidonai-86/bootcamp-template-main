"""
## Даг загрузки данных из API и подсчётом строк в файлах (S3/MinIO)
"""

import io
import posixpath
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def _build_s3_key(prefix: str, filename: str) -> str:
    prefix = (prefix or "").strip().strip("/")
    return posixpath.join(prefix, filename)


# ====== tasks ======
def api_request(
    http_conn_id: str, data_dir: str, bucket_name: str, aws_conn_id: str, **_context
):
    chunk_size = 60

    http_hook = HttpHook(method="GET", http_conn_id=http_conn_id)
    response = http_hook.run("/posts")
    response.raise_for_status()
    json_data = response.json()

    json_data_prt1 = json_data[:chunk_size]
    json_data_prt2 = json_data[chunk_size:]

    df1 = pd.DataFrame(json_data_prt1)
    df2 = pd.DataFrame(json_data_prt2)

    file1_name = "api_data_prt1.csv"
    file2_name = "api_data_prt2.csv"

    file1_key = _build_s3_key(data_dir, file1_name)
    file2_key = _build_s3_key(data_dir, file2_name)

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    # --- part 1 ---
    buffer1 = io.BytesIO()
    df1.to_csv(buffer1, index=False, encoding="utf-8", sep="~")
    buffer1.seek(0)
    s3_hook.load_bytes(
        bytes_data=buffer1.read(),
        key=file1_key,
        bucket_name=bucket_name,
        replace=True,
    )

    # --- part 2 ---
    buffer2 = io.BytesIO()
    df2.to_csv(buffer2, index=False, encoding="utf-8", sep="~")
    buffer2.seek(0) 
    s3_hook.load_bytes(
        bytes_data=buffer2.read(),
        key=file2_key,
        bucket_name=bucket_name,
        replace=True,
    )

    print(f"==== Данные чанка №1 записаны в s3://{bucket_name}/{file1_key} ====")
    print(f"==== Данные чанка №2 записаны в s3://{bucket_name}/{file2_key} ====")


def _validate_file(data_dir: str, file_marker: str, bucket_name: str, aws_conn_id: str):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    key = _build_s3_key(data_dir, f"api_data_{file_marker}.csv")

    if not s3_hook.check_for_key(key=key, bucket_name=bucket_name):
        raise FileNotFoundError(f"Не найден объект в S3: s3://{bucket_name}/{key}")

    # Читаем объект из S3 в память
    obj = s3_hook.get_key(key=key, bucket_name=bucket_name)
    body_bytes = obj.get()["Body"].read()
    if not body_bytes:
        raise ValueError(f"Файл в S3 пустой: s3://{bucket_name}/{key}")

    # pandas читает из буфера
    df = pd.read_csv(io.BytesIO(body_bytes), sep="~", encoding="utf-8")

    print(f"========= Файл api_data_{file_marker}.csv: {len(df)} строк =========")


def validate_files_prt1(data_dir: str, bucket_name: str, aws_conn_id: str, **_context):
    _validate_file(
        data_dir=data_dir,
        file_marker="prt1",
        bucket_name=bucket_name,
        aws_conn_id=aws_conn_id,
    )


def validate_files_prt2(data_dir: str, bucket_name: str, aws_conn_id: str, **_context):
    _validate_file(
        data_dir=data_dir,
        file_marker="prt2",
        bucket_name=bucket_name,
        aws_conn_id=aws_conn_id,
    )


# ====== DAG ======
default_args = {
    "owner": "x4Luck",
    "start_date": datetime(2026, 2, 21),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="x4Luck_airflow_pt1",
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    max_active_runs=1,
    description="API download",
    tags=["api"],
) as dag:
    DATA_DIR_PREFIX = Variable.get("x4Luck_DATA_DIR_API")
    BUCKET_NAME = "dev"
    AWS_CONN_ID = "minios3_conn"

    download = PythonOperator(
        task_id="download",
        python_callable=api_request,
        op_kwargs={
            "http_conn_id": "x4Luck_api_jsonplaceholder",
            "data_dir": DATA_DIR_PREFIX,
            "bucket_name": BUCKET_NAME,
            "aws_conn_id": AWS_CONN_ID,
        },
    )

    validate_1 = PythonOperator(
        task_id="validate_1",
        python_callable=validate_files_prt1,
        op_kwargs={
            "data_dir": DATA_DIR_PREFIX,
            "bucket_name": BUCKET_NAME,
            "aws_conn_id": AWS_CONN_ID,
        },
    )

    validate_2 = PythonOperator(
        task_id="validate_2",
        python_callable=validate_files_prt2,
        op_kwargs={
            "data_dir": DATA_DIR_PREFIX,
            "bucket_name": BUCKET_NAME,
            "aws_conn_id": AWS_CONN_ID,
        },
    )

    download >> [validate_1, validate_2]

dag.doc_md = __doc__
