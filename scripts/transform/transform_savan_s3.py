from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
    to_date,
    trim,
    date_format
)
from clickhouse_manager import ClickHouseManager
from s3_file_manager import S3FileManager
from typing import Iterator, List
import os

jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
db_host = os.getenv("CLICKHOUSE_HOST")
db_user = os.getenv("CLICKHOUSE_USER")
db_password = os.getenv("CLICKHOUSE_PASSWORD")
table_name = f"{os.getenv('TABLE_NAME')}_local"
distributed_table_name = os.getenv("TABLE_NAME")
s3_path_manifest = f"{os.getenv('S3_PATH_MANIFEST')}"
s3_path_eatherquake= os.getenv("S3_PATH_eatherquake")

# Инициализация Spark
spark = (
    SparkSession.builder.appName("S3ToChWeaherTempRain")
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
    host=db_host, user=db_user, password=db_password, database="sava_n"
)

print(f"table_name ===== {table_name}")


cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS sava_n.{table_name} ON CLUSTER {cluster_name} (
        magnitude Float64,
        place String,
        longitude Float64,
        latitude Float64,
        depth_km Float64,
        time_utc DateTime,           
        business_date Date                   
    )
    ENGINE = ReplacingMergeTree()
    PARTITION BY toYYYYMM(time_utc)
    ORDER BY (business_date, place, time_utc)
    """


create_distributed = f"""
    CREATE TABLE IF NOT EXISTS sava_n.{distributed_table_name}
    AS sava_n.{table_name}
    ENGINE = Distributed('{cluster_name}', 'sava_n', '{table_name}', rand()); 
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


    final = df.select(
        col("magnitude"),
        trim(col("place")).alias("place"),
        col("longitude"),
        col("latitude"),
        col("depth_km"),
        to_timestamp(trim(col("time_utc")), "yyyy-MM-dd HH:mm:ss").alias("time_utc"),
        to_date(trim(col("business_date")), "yyyy-MM-dd").alias("business_date")
    )

    # Запись в ClickHouse
    (
        final.write.format("jdbc")
        .option("url", jdbc_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", distributed_table_name)
        .mode("append")
        .save()
    )

    print(f"Кол-во срок == {final.count()}")

    print("✅ Батч успешно загружен в ClickHouse.")
