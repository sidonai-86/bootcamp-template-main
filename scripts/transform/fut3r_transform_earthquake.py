from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, split, trim, lower, md5, coalesce, date_format, current_timestamp
from s3_file_manager import S3FileManager
from clickhouse_manager import ClickHouseManager
from datetime import datetime, timedelta, timezone
import os


jdbc_url = os.getenv('CLICKHOUSE_JDBC_URL')
db_host = os.getenv('CLICKHOUSE_HOST')
db_user = os.getenv('CLICKHOUSE_USER')
db_password = os.getenv('CLICKHOUSE_PASSWORD')
table_name = "earthquakes_local"
#s3_path_regions = os.getenv('S3_PATH_REGIONS')
s3_path_earthquake = os.getenv('S3_PATH_EARTHQUAKE')
distributed_table_name = "earthquakes"


# Инициализация Spark
spark = SparkSession.builder \
    .appName("S3ToChEarthquake") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()


# Инициализация S3 менеджера
s3_manager = S3FileManager(
    bucket_name="dev",
    aws_access_key=os.getenv("MINIO_ROOT_USER"),
    aws_secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    endpoint_url="http://minio:9000",
)

# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=db_host,
    user=db_user,
    password=db_password,
    database="fut3r"
)

cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS fut3r.{table_name} ON CLUSTER {cluster_name} (
        id String,
        place String,
        region String,
        magnitude Float64,
        tsunami Nullable(Int32),
        url String,
        longitude Float64,
        latitude Float64,
        depth Float64,
        load_date Date,
        updated_at DateTime
    )
    ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(load_date)
    ORDER BY (load_date, id)
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS fut3r.{distributed_table_name}
    ENGINE = Distributed('{cluster_name}', 'fut3r', '{table_name}', rand())
    """

print(create_main)
print(create_distributed)

ch_manager.execute_sql(create_main)
ch_manager.execute_sql(create_distributed)

print("Таблицы созданы")

# Получение максимальной даты обновления
max_updated = ch_manager.get_max_updated_at(table_name)
max_updated = max_updated.replace(tzinfo=timezone.utc)

print(f"✅ max_updated {max_updated}")

# Получаем список новых файлов из S3
new_files = s3_manager.list_files_newer_than(
    prefix="fut3r/earthquakes/",
    update_at=max_updated
)

print(f"Найдено {len(new_files)} новых файлов для обработки")

if not new_files:
    print("Нет новых файлов для обработки")
    spark.stop()
    exit(0)

# Читаем данные
df = spark.read.parquet(*new_files)
#regions = spark.read.parquet(s3_path_regions)

# Преобразование и обогащение
df_transf = (df.select(
        col("id"),
        col("created_at").cast("date").alias("load_date"),
        trim(split(col("`properties.place`"), ",").getItem(0)).alias("place"),
        trim(split(col("`properties.place`"), ",").getItem(1)).alias("region"),
        col("`properties.mag`").alias("magnitude"),
        col("`properties.tsunami`").alias("tsunami"),
        col("`properties.url`").alias("url"),
        col("`geometry.coordinates`")[0].alias("longitude"),
        col("`geometry.coordinates`")[1].alias("latitude"),
        col("`geometry.coordinates`")[2].alias("depth"),
    )
    .withColumn("updated_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")))

# Запись в ClickHouse
df_transf.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("dbtable", distributed_table_name) \
    .mode("append") \
    .save()

print(f"Кол-во срок == {df_transf.count()}")

print("✅ Данные успешно загружены в ClickHouse.")