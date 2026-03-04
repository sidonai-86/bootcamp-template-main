#!/usr/bin/env python
# coding: utf-8

# In[83]:


from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import requests
import pyarrow
import pandas as pd
import datetime as dt
import boto3 
import io
from datetime import datetime, timezone
# Импорты для Airflow


# In[84]:


import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("SparkExample") \
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
        "org.postgresql:postgresql:42.5.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        ) \
    .getOrCreate()


hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.path.style.access", "true")

# Импорты и настройки для Spark, Hadoop


# In[85]:


load_date = dt.date.today()
# url = "https://api.open-meteo.com/v1/forecast"


# In[86]:


def check_new_files(**context):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )

    prefix = "artemw9"
    bucket = "dev"

    # Получаем последнее время проверки из Airflow Variable или используем start_date
    last_check = Variable.get(
        "LAST_S3_CHECK_TIME__WEATHER", default_var="2020-01-01T00:00:00"
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
    Variable.set("LAST_S3_CHECK_TIME__WEATHER", now_str)

    manifest_content = "\n".join(path.strip() for path in new_files if path.strip())
    manifest_key = "artemw9/manifests/new_files.txt"

    hook = S3Hook(aws_conn_id="minios3_conn")
    hook.load_string(
        string_data=manifest_content,
        key=manifest_key,
        bucket_name="dev",
        replace=True,
    )

    context["ti"].xcom_push(key="manifest_key", value=manifest_key)
    context["ti"].xcom_push(key="count_new_files", value=len(new_files))


# In[87]:


def filter_values (**context):
    ti = context["ti"]
    
    manifest_key = ti.xcom_pull(task_ids='check_new_files', key='manifest_key')
    
    hook = S3Hook(aws_conn_id="minios3_conn")
    manifest_content = hook.read_key(key=manifest_key, bucket_name="dev")
    files_to_process = manifest_content.strip().split('\n')
    
    all_data = []
    processed_files = []
    
    for file_key in files_to_process:

        
        file_obj = hook.get_key(key=file_key, bucket_name="dev")
        parquet_buffer = io.BytesIO(file_obj.get()['Body'].read())
        df = pd.read_parquet(parquet_buffer)
        
        # Проверяем наличие hourly данных
        if 'hourly.time' not in df.columns:
            print(f"⚠️ В файле {file_key} нет hourly данных")
            continue
        
        # Создаем DataFrame из списков
        hourly_data = pd.DataFrame({
            'datetime': df['hourly.time'].iloc[0],  # берем первый (и единственный) элемент
            'cloud_cover': df['hourly.cloud_cover'].iloc[0],
            'surface_pressure': df['hourly.surface_pressure'].iloc[0],
            'weather_code': df['hourly.weather_code'].iloc[0]
        })
        
        
        # Дополнительная фильтрация по значениям
        df_filtered = hourly_data[
            (hourly_data['cloud_cover'] >= 0) &  # только валидные значения
            (hourly_data['surface_pressure'] > 900)  # давление выше 900 гПа
        ]
    
        
        if len(df_filtered) > 0:
            all_data.append(df_filtered)
    
    final_df = pd.concat(all_data, ignore_index=True)
    print(final_df.head())
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename_parquet = f"artemw9/api_artemw9_filtered_{load_date}.parquet"
    
    buffer = io.BytesIO()
    final_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    hook.load_bytes(
        bytes_data=buffer.read(), 
        key=filename_parquet, 
        bucket_name="dev", 
        replace=True
    )
    processed_files.append(filename_parquet)
    # Создаём новый манифест

    processed_manifest_key = f"artemw9/manifests/processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    hook.load_string(
        string_data="\n".join(processed_files),  # ← ПРЕОБРАЗУЕМ список в строку
        key=processed_manifest_key,
        bucket_name="dev",
        replace=True
    )


    ti.xcom_push(key="processed_manifest_key", value=processed_manifest_key)
    ti.xcom_push(key="load_date", value=timestamp)
    
    print(f"✅ Отфильтровано и сохранено: {len(final_df)} строк")
    







# In[88]:


default_args = {
    "owner": "loader",
    "start_date": days_ago(30),
    "retries": 5,
}

dag = DAG(
    dag_id="artemw9_belyakov_S3_to_CH",
    default_args=default_args,
    schedule="0 1 * * *",  # каждый день в 00:00 UTC
    catchup=True,
    description="From S3 to CH",
    tags=["weather", "clouds", "pressure", "code","s3 -> ch", ],
)


# In[89]:


dag.doc_md = __doc__


# In[90]:


s3_ch = SparkSubmitOperator(
    task_id="spark_s3_to_ch",
    application="/opt/airflow/scripts/transform/transform__weather_cloud_pressure.py",
    conn_id="spark_default",
    env_vars={
        "CLICKHOUSE_JDBC_URL": "jdbc:clickhouse://clickhouse01:8123/default",
        "CLICKHOUSE_HOST": "clickhouse01",
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        "TABLE_NAME": "weather_temp_rain",
        "S3_PATH_MANIFEST": "{{ ti.xcom_pull(task_ids='filter_values', key='processed_manifest_key') }}",
        "S3_PATH_WEATHER_TEMP_RAIN": "s3a://prod/api/weather_temp_rain",
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


# In[91]:


check_new_files = PythonOperator(
    task_id="check_new_files", python_callable=check_new_files, dag=dag
)

filter_task = PythonOperator(task_id="filter_values", python_callable=filter_values, dag=dag)



# In[92]:


with TaskGroup(group_id="notifications", dag=dag) as notifications:
    send_email = EmailOperator(
        task_id="send_email",
        to="artembelakov66@gmail.com",
        subject="Weather parquet загружен за {{ ti.xcom_pull(task_ids='filter_values', key='load_date') }}",
        html_content="""
            <p>Файл <b>kz_{{ ti.xcom_pull(task_ids='filter_values', key='load_date') }}.parquet</b> успешно положен в S3.</p>
            <p>Путь: <code>s3://dev/api/artemw9/kz_{{ ti.xcom_pull(task_ids='filter_values', key='load_date') }}.parquet</code></p>
            <p>Кол-во новых файлов: {{ti.xcom_pull(task_ids='check_new_files', key='count_new_files')}}</p>
        """,
        conn_id="artemw9_mail",
        dag=dag,
    )

    send_message_telegram = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id="artemw9_tg",
        chat_id="{{ var.value.Artemw9_TELEGRAM_CHAT_ID }}",  # ..."chat":{"id":CHAT_ID,"firs...
        text=(
            "✅ Погода за {{ ti.xcom_pull(task_ids='filter_values', key='load_date') }} "
            "загружена\n"
            "Файл: <code>kz_{{ ti.xcom_pull(task_ids='filter_values', key='load_date') }}.parquet</code>\n"
            "Кол-во новых файлов: {{ti.xcom_pull(task_ids='check_new_files', key='count_new_files')}}"
        ),
        dag=dag,
    )
check_new_files >> filter_task >> s3_ch >> notifications

