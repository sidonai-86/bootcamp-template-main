from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col,
#     current_timestamp,
#     explode,
#     arrays_zip,
#     unix_timestamp,
#     from_unixtime,
#     date_format,
# )
from clickhouse_manager import ClickHouseManager
from s3_file_manager import S3FileManager
from typing import Iterator, List
import os
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import FloatType, DoubleType, IntegerType

jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
db_host = os.getenv("CLICKHOUSE_HOST")
db_user = os.getenv("CLICKHOUSE_USER")
db_password = os.getenv("CLICKHOUSE_PASSWORD")
table_name = f"{os.getenv('TABLE_NAME')}_local"
distributed_table_name = os.getenv("TABLE_NAME")
s3_path_manifest = f"{os.getenv('S3_PATH_MANIFEST')}"
s3_path_eartquakes = os.getenv("S3_PATH_EARTQUAKES")
db_name = "aidarai"


# Инициализация Spark
spark = (
    SparkSession.builder.appName("aidarai_S3ToChEartquakes")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)

# Инициализация S3 менеджера
s3_manager = S3FileManager(
    bucket_name=os.getenv("MINIO_DEV_BUCKET_NAME"),
    aws_access_key=os.getenv("MINIO_ROOT_USER"),
    aws_secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    endpoint_url="http://minio:9000",
)

# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=db_host, user=db_user, password=db_password, database=db_name
)

print(f"table_name ===== {table_name}")

cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ON CLUSTER {cluster_name} (
        id String,
        time DateTime64(0),
        time_upd DateTime64(0),
        mag Float32,
        mag_type LowCardinality(String),
        latitude Float64,
        longitude Float64,
        depth Float32,
        place String,
        status LowCardinality(String),
        alert LowCardinality(String),
        tsunami UInt8,
        sig Int32,
        url String,
        update_at DateTime64(0)
    )
    ENGINE = ReplacingMergeTree(update_at)
    PARTITION BY toYYYYMM(time)
    ORDER BY (time)
    """


create_distributed = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{distributed_table_name}
    AS {db_name}.{table_name}
    ENGINE = Distributed('{cluster_name}', '{db_name}', '{table_name}', halfMD5(time));
    """

print(create_main)
print(create_distributed)

ch_manager.execute_sql(create_main)
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

    df_transformed = df.select(
        col("id").cast("string").alias("id"),

        # миллисекунды → секунды
        from_unixtime(col("properties_time") / 1000)
            .cast("timestamp")
            .alias("time"),

        from_unixtime(col("properties_updated") / 1000)
            .cast("timestamp")
            .alias("time_upd"),

        col("properties_mag")
            .cast(FloatType())
            .alias("mag"),

        col("properties_magType")
            .alias("mag_type"),

        col("latitude")
            .cast(DoubleType())
            .alias("latitude"),

        col("longitude")
            .cast(DoubleType())
            .alias("longitude"),

        col("depth")
            .cast(FloatType())
            .alias("depth"),

        col("properties_place")
            .alias("place"),

        col("properties_status")
            .alias("status"),

        col("properties_alert")
            .alias("alert"),

        col("properties_tsunami")
            .cast(IntegerType())   # будет UInt8 в CH
            .alias("tsunami"),

        col("properties_sig")
            .cast(IntegerType())
            .alias("sig"),

        col("properties_url")
            .alias("url"),

        # уже есть в parquet
        col("update_at")
            .cast("timestamp")
            .alias("update_at")
    )


    # Запись в ClickHouse
    (
        df_transformed.write.format("jdbc")
        .option("url", jdbc_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("dbtable", distributed_table_name)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode("append")
        .save()
    )

    print(f"Кол-во срок == {df_transformed.count()}")

    print("✅ Батч успешно загружен в ClickHouse.")
