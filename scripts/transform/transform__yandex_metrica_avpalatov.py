from pyspark.sql import SparkSession
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


jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
db_host = os.getenv("CLICKHOUSE_HOST")
db_user = os.getenv("CLICKHOUSE_USER")
db_password = os.getenv("CLICKHOUSE_PASSWORD")
table_name = f"{os.getenv('TABLE_NAME')}_local"
distributed_table_name = os.getenv("TABLE_NAME")
s3_path_manifest = f"{os.getenv('S3_PATH_MANIFEST')}"
# s3_path = os.getenv("S3_PATH")


# Инициализация Spark
spark = (
    SparkSession.builder.appName("S3ToChYM")
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

ch_database = "avpalatov"

# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=db_host, user=db_user, password=db_password, database=ch_database
)

print(f"table_name ===== {table_name}")

cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS {ch_database}.{table_name} ON CLUSTER {cluster_name}
    (
        date           Date                   COMMENT 'Дата',
        country        LowCardinality(String) COMMENT 'Страна',
        city           LowCardinality(String) COMMENT 'Город', 
        language       LowCardinality(String) COMMENT 'Язык',
        browser        LowCardinality(String) COMMENT 'Браузер',
        device         LowCardinality(String) COMMENT 'Устройство',
        os             LowCardinality(String) COMMENT 'ОС',
        hour           LowCardinality(String) COMMENT 'Час (00:00-23:00)',
        visits         UInt64                 COMMENT 'Посещения',
        users          UInt64                 COMMENT 'Пользователи',
        new_users      UInt64                 COMMENT 'Новые пользователи',
        page_views     UInt64                 COMMENT 'Просмотры страниц',
        update_at      Date 					COMMENT 'Время загрузки'
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{table_name}', '{{replica}}')
    PARTITION BY toYYYYMM(date)
    ORDER BY (date, hour, country, city, device)
    PRIMARY KEY (date, hour, country, city)
    SETTINGS 
        index_granularity = 8192,
        storage_policy = 'default'
    COMMENT 'Веб аналитика: посещения по часам'
    """


create_distributed = f"""
    CREATE TABLE IF NOT EXISTS {ch_database}.{distributed_table_name}
    AS {ch_database}.{table_name}
    ENGINE = Distributed('{cluster_name}', '{ch_database}', '{table_name}', halfMD5(date, country, city));
    """

print(create_main)
print(create_distributed)

ch_manager.execute_sql(create_main)
ch_manager.execute_sql(create_distributed)

print("Таблицы созданы (перестраховка)")
print(s3_path_manifest)


# Трансформация на Spark
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

    renamed_df = (df
                .withColumnRenamed("ym:s:date", "date")
                .withColumnRenamed("ym:s:regionCountry", "country")
                .withColumnRenamed("ym:s:regionCity", "city")
                .withColumnRenamed("ym:s:browserLanguage", "language")
                .withColumnRenamed("ym:s:browser", "browser")
                .withColumnRenamed("ym:s:deviceCategory", "device")
                .withColumnRenamed("ym:s:operatingSystem", "os")
                .withColumnRenamed("ym:s:hour", "hour")
                .withColumnRenamed("ym:s:visits", "visits")
                .withColumnRenamed("ym:s:users", "users")
                .withColumnRenamed("ym:s:newUsers", "new_users")
                .withColumnRenamed("ym:s:pageviews", "page_views")
    )

    # Запись в ClickHouse
    (
        renamed_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("user", db_user)
            .option("password", db_password)
            .option("dbtable", table_name)
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .option("dbtable", distributed_table_name)
            .mode("append")
            .save()
    )

    print(f"Кол-во срок == {renamed_df.count()}")

    print("✅ Батч успешно загружен в ClickHouse.")
