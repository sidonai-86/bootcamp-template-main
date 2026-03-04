from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from s3_file_manager import S3FileManager
from clickhouse_manager import ClickHouseManager
import os


jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
db_host = os.getenv("CLICKHOUSE_HOST")
db_user = os.getenv("CLICKHOUSE_USER")
db_password = os.getenv("CLICKHOUSE_PASSWORD")
table_name = f"{os.getenv('TABLE_NAME')}_local"
s3_path_regions = os.getenv("S3_PATH_REGIONS")
s3_path_earthquake = os.getenv("S3_PATH_EARTHQUAKE")
distributed_table_name = os.getenv("TABLE_NAME")


# Инициализация Spark
spark = (
    SparkSession.builder.appName("S3ToChEarthquake")
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
    host=db_host, user=db_user, password=db_password, database="vadng"
)

cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS vadng.{table_name} ON CLUSTER {cluster_name} (
        id String,
        time Datetime,
        latitude Float64,
        longtitude Float64,
        depth Float64,
        place String,
        mag Float32,
        magType FixedString(5),
        tsunami Boolean,
        updated_at DateTime
    )
    ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(time)
    ORDER BY (toDate(time), latitude, longtitude)
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS default.{distributed_table_name} ON CLUSTER {cluster_name}
    AS vadng.{table_name}
    ENGINE = Distributed('{cluster_name}', 'vadng', '{table_name}', rand())
    """

print(create_main)
print(create_distributed)

ch_manager.execute_sql(create_main)
ch_manager.execute_sql(create_distributed)

print("Таблицы созданы")

# Получение максимальной даты обновления
max_updated = ch_manager.get_max_updated_at(table_name)

print(f"✅ max_updated {max_updated}")

# Получаем список новых файлов из S3
new_files = s3_manager.list_files_newer_than(
    prefix="vadng/API_to_S3/", update_at=max_updated
)

print(new_files)
print(f"Найдено {len(new_files)} новых файлов для обработки")

if not new_files:
    print("Нет новых файлов для обработки")
    spark.stop()
    exit(0)

# Читаем данные
df = spark.read.parquet(*new_files)

df = df.withColumn("time", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("updated_at", to_timestamp(col("updated_at"), "yyyy-MM-dd HH:mm:ss"))

# Запись в ClickHouse
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("dbtable", table_name) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .mode("append") \
    .save()

print(f"Кол-во срок == {df.count()}")

print("✅ Данные успешно загружены в ClickHouse.")
