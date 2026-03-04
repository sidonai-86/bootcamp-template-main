import os
import sys
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from s3_file_manager import S3FileManager

import pyspark.sql.functions as F

GP_HOST = os.environ.get("GP_HOST")
GP_PORT = os.environ.get("GP_PORT")
GP_TABLE = "fut3r.books"
GP_DB = os.environ.get("GP_DB")
GP_USER = os.environ.get("GP_USER")
GP_PASSWORD = os.environ.get("GP_PASSWORD")

JDBC_URL = f"jdbc:postgresql://{GP_HOST}:{GP_PORT}/{GP_DB}"
S3_BUCKET_PATH = "s3a://dev/fut3r/books/"

s3_manager = S3FileManager(
    bucket_name="dev",
    aws_access_key=os.getenv("MINIO_ROOT_USER"),
    aws_secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    endpoint_url="http://minio:9000",
)

# Инициализация Spark
spark = SparkSession.builder \
    .appName("CSVTransform") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

max_updated = (datetime.now() - timedelta(days=1)).replace(tzinfo=timezone.utc) #max_updated.replace(tzinfo=timezone.utc)

print(f"✅ max_updated {max_updated}")

# Получаем список новых файлов из S3
new_files = s3_manager.list_files_newer_than(
    prefix="fut3r/books/",
    update_at=max_updated
)

print(f"Найдено {len(new_files)} новых файлов для обработки")

if not new_files:
    print("Нет новых файлов для обработки")
    spark.stop()
    exit(0)

# Читаем данные
df = spark.read.parquet(*new_files)

#Читаем данные
df = spark.read.parquet(f"{S3_BUCKET_PATH}/*.parquet")

df_exploded = df.select('*', F.explode('changes').alias('changed'))
df = df_exploded.select(
    F.col('id'),
    F.col('kind'),
    F.col('timestamp').cast("timestamp").alias('ts'),
    F.col('comment'),
    F.col('ip'),
    F.col('`author.key`').alias('author_key'),
    F.col('`data.master`').alias('data_master'),
    F.when(
        F.col('`data.duplicates`').isNotNull(), F.concat_ws(", ", F.col('`data.duplicates`'))
    ).alias('data_duplicates'),
    F.col("changed.key").alias("item_key"),
    F.col("changed.revision").alias("rev_number"),
    F.col('updated_at').cast("date")
)

df.write \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", GP_TABLE) \
    .option("user", GP_USER) \
    .option("password", GP_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print(f"Кол-во срок == {df.count()}")

print("✅ Данные успешно загружены в Greenplum.")