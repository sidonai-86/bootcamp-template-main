import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.db import provide_session
from airflow.models import DagRun
from airflow.utils.dates import days_ago


default_args = {
    "owner": "vvv_Nika",
    "start_date": days_ago(3),
    "retries": 1,
}

dag = DAG(
    dag_id="vvv_Nika__S3_to_CLick_weather",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    description="Spark Submit Inc",
    catchup=False,
    tags=["spark", "transform"],
)

@provide_session
def check_api_to_s3_done(session=None, **kwargs):
    dag_runs = (
        session.query(DagRun)
        .filter(DagRun.dag_id == "vvv_Nika__API_to_S3_test", DagRun.state == "success")
        .all()
    )
    return len(dag_runs) > 0


wait_api_to_s3_dag = PythonSensor(
    task_id="wait_for_api_to_s3_loader_any_success",
    python_callable=check_api_to_s3_done,
    poke_interval=60,
    timeout=60 * 60 * 12,
    mode="poke",
    dag=dag,
)

s3_to_ch = SparkSubmitOperator(
    task_id="spark_s3_to_ch",
    application="/opt/airflow/scripts/transform/vvv_transform_s3_to_click_weather.py",           
    conn_id="spark_default",
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/vvv_db",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "TABLE_NAME": "weather",                                                 
        "S3_PATH_WEATHER": f's3a://{os.getenv("MINIO_DEV_BUCKET_NAME")}/vvv_Nika/API_to_S3/',     
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

wait_api_to_s3_dag >> s3_to_ch

