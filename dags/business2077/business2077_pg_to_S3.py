from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import io
import pandas as pd
import requests
from datetime import datetime

import os
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import Window


def api_download(**context):

    ds=context["ds"]

    ti = context["ti"]


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



    jdbc_url = "jdbc:postgresql://postgres_source:5432/source"
    table_name = "public.shops"
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")


    shops_df = (
            spark
            .read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("user", db_user)
            .option("password", db_password)
            .option("dbtable", table_name)
            .option("fetchsize", 1000)
            .option("driver", "org.postgresql.Driver")
            .load()
            )


    
    jdbc_url = "jdbc:postgresql://postgres_source:5432/source"
    table_name = "public.shop_timezone"
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")


    shop_timezone_df = (
            spark
            .read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("user", db_user)
            .option("password", db_password)
            .option("dbtable", table_name)
            .option("fetchsize", 1000)
            .option("driver", "org.postgresql.Driver")
            .load()
            )

    
    

    sz = (
        shop_timezone_df
        .where(F.col('plant').rlike('^[0-9]+$'))
        .withColumnRenamed('plant','id')
        .withColumn('id',F.col('id').cast('int').alias('id'))
    )
    s=(
    shops_df
    .withColumn('st_id',F.col('st_id').cast('int')) 
    )
    cte=(
        s
        .join(sz, s.st_id==sz.id)
        .withColumn('rnk', F.row_number().over(Window.partitionBy('st_id').orderBy(F.col('time_zone').desc())))
    )
    res=(
        cte
        .where(F.col('rnk')==1)
        .withColumn('time_zone',F.when((F.col('time_zone')=='' )| (F.col('time_zone').isNull()),3).otherwise(F.col('time_zone').substr(4,10)).cast('int'))
        .select('st_id','shop_name','time_zone')
    )
    # sz.printSchema()
    # sz.show()
    # s.printSchema()
    # s.show()
    # cte.printSchema()
    # cte.show()
    # res.printSchema()
    # res.show()


    s3_path = f"s3a://dev/business2077/business2077_pg_to_S3_/pg_data_{ds}.parquet"

    
    (
        res.write
        .mode("overwrite") 
        .parquet(s3_path)
    )

    ti.xcom_push(key="load_date", value=ds)

    spark.stop()
    


default_args = {
"owner": "business2077",
"start_date": days_ago(3),
"retries": 2,
}

dag = DAG(
    dag_id="busines2077__pg_to_S3",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # каждый день в 03:00 UTC
    catchup=True,#если true то запускаются все прошедшие даты начиная с начала расписания, false-будет запускать начиная с последнего
    description="dag description",
    tags=["pg"],
)

upload = PythonOperator(task_id="upload", python_callable=api_download, dag=dag)

send_message_telegram = TelegramOperator(
    task_id="send_message_telegram",
    telegram_conn_id="busines2077_tg",
    chat_id="{{ var.value.BUSINESS2077_TELEGRAM_CHAT_ID }}",  
    text=(
        "✅ Данные из pg за {{ ti.xcom_pull(task_ids='upload', key='load_date') }} "
        "загружены\n"
        "Файл: <code>pg_data_{{ ti.xcom_pull(task_ids='upload', key='load_date') }}.parquet</code>\n"
    ),
    dag=dag,
)

upload >> send_message_telegram



