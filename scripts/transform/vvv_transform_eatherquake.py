from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    split,
    trim,
    from_unixtime,
    current_timestamp,
    to_timestamp
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
s3_path_weather_temp_rain = os.getenv("S3_PATH_EATHERQUAKE")


# Инициализация Spark
spark = (
    SparkSession.builder.appName("S3ToChEatherQuake")
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
    host=db_host, user=db_user, password=db_password, database="vvv_db"
)

print(f"table_name ===== {table_name}")

cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS vvv_db.{table_name} ON CLUSTER {cluster_name} (
        id String,
        ts DateTime,
        place String,
        region String,
        magnitude Float64,
        felt Nullable(Int32),
        tsunami Nullable(Int32),
        url String,
        longitude Float64,
        latitude Float64,
        depth Float64,
        load_date Date,
        update_at DateTime
    ) ENGINE = ReplacingMergeTree(update_at)
    PARTITION BY toYYYYMM(load_date)
    ORDER BY (load_date, id)
    """


create_distributed = f"""
    CREATE TABLE IF NOT EXISTS vvv_db.{distributed_table_name}
    AS vvv_db.{table_name}
    ENGINE = Distributed('{cluster_name}', 'vvv_db', '{table_name}', rand());
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

    final = df.selectExpr(
        "id",
        "from_unixtime(`properties.time` / 1000) as ts",
        "trim(split(`properties.place`, ',')[0]) as place",
        "trim(split(`properties.place`, ',')[1]) as region",
        "`properties.mag` as magnitude",
        "`properties.felt` as felt",
        "`properties.tsunami` as tsunami",
        "`properties.url` as url",
        "`geometry.coordinates`[0] as longitude",
        "`geometry.coordinates`[1] as latitude",
        "`geometry.coordinates`[2] as depth",
        "cast(bisness_date as date) as load_date"
    ).withColumn(
        "update_at", 
        to_timestamp(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))  
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

