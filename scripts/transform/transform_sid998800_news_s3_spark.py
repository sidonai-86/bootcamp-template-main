from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    arrays_zip,
    unix_timestamp,
    from_unixtime,
    date_format,
)

from clickhouse_manager import ClickHouseManager
from s3_file_manager import S3FileManager
from typing import Iterator, List
import os
import time

jdbc_url = "jdbc:clickhouse://clickhouse01:8123/sid998800"
db_host = "clickhouse01"
db_user = os.getenv("CLICKHOUSE_USER")
db_password = os.getenv("CLICKHOUSE_PASSWORD")
table_name = "s3_ch_news_local"
distributed_table_name = "s3_ch_news"
s3_path_manifest = f"{os.getenv('S3_PATH_MANIFEST')}"
s3_path_weather_temp_rain = os.getenv("S3_PATH_WEATHER_TEMP_RAIN")

# Инициализация Spark S3ToChNews
spark = SparkSession.builder \
    .appName("S3ToChNews") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

# Инициализация S3 менеджера
s3_manager = S3FileManager(
    bucket_name=os.getenv("MINIO_DEV_BUCKET_NAME"),
    aws_access_key=os.getenv("MINIO_ROOT_USER"),
    aws_secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    endpoint_url="http://minio:9000",
)

# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=db_host, user=db_user, password=db_password, database="sid998800"
)
print(f"table_name ===== {table_name}")

cluster_name = "company_cluster"
database = "sid998800"

create_main = f"""
    CREATE TABLE IF NOT EXISTS {database}.{table_name} ON CLUSTER {cluster_name} (
        author String,
        title String,
        description String,
        url String,
        source_name String,
        published_at DateTime,
        updated_at DateTime
    )
    ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(published_at)
    ORDER BY (published_at, author)
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS {database}.{distributed_table_name} ON CLUSTER {cluster_name}
    AS {database}.{table_name}
    ENGINE = Distributed('{cluster_name}', '{database}', {table_name}, halfMD5(published_at));
    """

print(create_main)
print(create_distributed)

ch_manager.execute_sql(create_main)
# Ждем секунду, чтобы Clickhouse успел обработать запрос на обоих кластерах и создаем дистрим таблицу
time.sleep(1)
ch_manager.execute_sql(create_distributed)

print("Таблицы созданы")
print(s3_path_manifest)

# Трансформаиця на Spark
def batch_generator_from_lines(
    lines: Iterator[str], batch_size=10
) -> Iterator[List[str]]:
    batch = []
    for line in lines:
        if line:
            batch.append(line)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

lines = s3_manager.stream_lines_from_s3(s3_path_manifest)

for batch_paths in batch_generator_from_lines(lines, 10):
    print("Загрузка в ClickHouse батчами по 10 файлов")
    df = spark.read.parquet(*batch_paths)

    # Переименовываем столбец с точкой в имени и publishedAt
    df_renamed = df.withColumnRenamed("source.name", "source_name")\
        .withColumnRenamed("source.id", "source_id") \
        .withColumnRenamed("publishedAt", "published_at")

    # Удалил колонку, т.к. в ней дубли из колонки source.name, а контенте каша из символов
    # Переводим столбцы с датами в одинакоый формат timestamp без милисекунд
    df_final = df_renamed.select(
        col("author"),
        col("title"),
        col("description"),
        col("url"),
        col("source_name"),
        col("published_at"),
        col("updated_at")
        ).drop("source_id", "content") \
        .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss").cast("timestamp")) \
        .withColumn("updated_at", date_format(col("updated_at"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))    

    # Запись в ClickHouse
    (
        df_final.write.format("jdbc")
        .option("url", jdbc_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", distributed_table_name)
        .mode("append")
        .save()
    )

    print(f"Кол-во срок == {df_final.count()}")

    print("✅ Батч успешно загружен в ClickHouse.")
