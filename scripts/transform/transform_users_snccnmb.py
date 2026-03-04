from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from clickhouse_manager import ClickHouseManager
import boto3
from io import BytesIO
import requests
import os


users_csv = {
    "google_users.csv": "https://huggingface.co/datasets/halltape/users/resolve/main/google_users.csv",
    "other_users.csv": "https://huggingface.co/datasets/halltape/users/resolve/main/other_users.csv",
    "yandex_users.csv": "https://huggingface.co/datasets/halltape/users/resolve/main/yandex_users.csv"
}

prefix = 'snccnmb/users_data'
bucket = 'dev'


def to_s3():
    s3_client = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD")
    )

    for filename, url in users_csv.items():

        response = requests.get(url)
        data = BytesIO(response.content)
        s3_client.upload_fileobj(
            data,
            bucket,
            f"{prefix}/{filename}"
        )


def from_s3():

    # Инициализация Spark
    spark = (
        SparkSession.builder.appName("users_ch")
        .config("spark.ui.port", "4041")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    base_path = f"s3a://{bucket}/{prefix}/"

    # Чтение файликов
    google_users = spark.read.option("header", "true").csv(f"{base_path}google_users.csv")
    other_users = spark.read.option("header", "true").csv(f"{base_path}other_users.csv")
    yandex_users = spark.read.option("header", "true").csv(f"{base_path}yandex_users.csv")

    # Трансформация по ТЗ
    df_final = (
            google_users
            .unionByName(other_users)
            .unionByName(yandex_users)
            .withColumnRenamed('tg_id', 'telegram_id')
            .withColumnRenamed('tg_nickname', 'user_nickname')
            .withColumnRenamed('update_at', 'registration_date')
            .select('telegram_id', 'user_nickname', 'registration_date')
            .withColumn('user_nickname', regexp_replace(col("user_nickname"), "@", ""))
            .withColumn('telegram_id', regexp_replace(col("telegram_id"), "@", ""))
            .withColumn('registration_date', to_timestamp(col('registration_date'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        )

    """ Часть работы с Кликом ↓ """
    jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
    db_host = os.getenv("CLICKHOUSE_HOST")
    db_user = os.getenv("CLICKHOUSE_USER")
    db_password = os.getenv("CLICKHOUSE_PASSWORD")

    cluster_name = 'company_cluster'
    database = 'snccnmb'

    ch_manager = ClickHouseManager(
        host=db_host, user=db_user, password=db_password, database=database
    )

    # Создание табличек
    drop_local = f"""
    DROP TABLE IF EXISTS {database}.users_local ON CLUSTER {cluster_name} SYNC
    """

    drop_dist = f"""
    DROP TABLE IF EXISTS {database}.users_dist ON CLUSTER {cluster_name} SYNC
    """
    create_main = f"""
        CREATE TABLE IF NOT EXISTS {database}.users_local ON CLUSTER {cluster_name}
        (
            telegram_id UInt64,
            user_nickname String,
            registration_date DateTime
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/events_users_{database}', '{{replica}}')
        ORDER BY (telegram_id)
        """

    create_distributed = f"""
        CREATE TABLE IF NOT EXISTS {database}.users_dist ON CLUSTER {cluster_name}
        AS {database}.users_local
        ENGINE = Distributed('{cluster_name}', '{database}', 'users_local', halfMD5(registration_date));
        """

    ch_manager.execute_sql(drop_dist)
    ch_manager.execute_sql(drop_local)
    ch_manager.execute_sql(create_main)
    ch_manager.execute_sql(create_distributed)

    (
        df_final.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", "users_dist")
        .mode("append")
        .save()
    )


to_s3()
from_s3()
