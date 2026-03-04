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
s3_path_weather_temp_rain = os.getenv("S3_PATH_WEATHER_TEMP_RAIN")


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
    host=db_host, user=db_user, password=db_password, database="business2077"
)

print(f"table_name ===== {table_name}")

cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS business2077.{table_name} ON CLUSTER {cluster_name} (
        time String,
        temperature_2m Float32,
        rain Float32,
        snow Float32,
        updated_at DateTime64(0)
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (time)
    """


create_distributed = f"""
    CREATE TABLE IF NOT EXISTS business2077.{distributed_table_name}
    AS business2077.{table_name}
    ENGINE = Distributed('{cluster_name}', 'business2077', '{table_name}', halfMD5(time));
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
    df.show()
    df.printSchema()

    # renamed_df = (
    #     df.withColumnRenamed("hourly.time", "hourly_time")
    #     .withColumnRenamed("hourly.temperature_2m", "hourly_temperature_2m")
    #     .withColumnRenamed("hourly.rain", "hourly_rain")
    # )

    # exploded_df = renamed_df.select(
    #     explode(
    #         arrays_zip("hourly_time", "hourly_temperature_2m", "hourly_rain")
    #     ).alias("zipped")
    # )

    # parsed_df = (
    #     exploded_df.select(
    #         col("zipped.hourly_time").cast("timestamp").alias("time"),
    #         col("zipped.hourly_temperature_2m").alias("temperature"),
    #         col("zipped.hourly_rain").alias("rain"),
    #     )
    #     .withColumn("load_date", col("time").cast("date"))
    #     .withColumn(
    #         "updated_at",
    #         from_unixtime(unix_timestamp(current_timestamp())).cast("timestamp"),
    #     )
    # )

    # parsed_df = (
    #     exploded_df.select(
    #         col("zipped.hourly_time").cast("string").alias("time"),
    #         col("zipped.hourly_temperature_2m").alias("temperature"),
    #         col("zipped.hourly_rain").alias("rain"),
    #     )
    #     .withColumn("weather_date", col("time").cast("date"))
    #     .withColumn(
    #         "updated_at",
    #         date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("timestamp"),
    #     )
    # )

    # Запись в ClickHouse
    # (
    #     df.write.format("jdbc")
    #     .option("url", jdbc_url)
    #     .option("user", db_user)
    #     .option("password", db_password)
    #     .option("dbtable", table_name)
    #     .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    #     .option("dbtable", distributed_table_name)
    #     .mode("append")
    #     .save()
    # )
    
    print(f"Кол-во срок == {df.count()}")

    print("✅ Батч успешно загружен в ClickHouse.")


