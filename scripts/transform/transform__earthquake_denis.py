from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, split, trim, lower, md5, coalesce, date_format, current_timestamp
from s3_file_manager import S3FileManager
from clickhouse_manager import ClickHouseManager
import os


jdbc_url = os.getenv('CLICKHOUSE_JDBC_URL')
db_host = os.getenv('CLICKHOUSE_HOST')
db_user = os.getenv('CLICKHOUSE_USER')
db_password = os.getenv('CLICKHOUSE_PASSWORD')
table_name = f"{os.getenv('TABLE_NAME')}_local"
distributed_table_name = os.getenv('TABLE_NAME')


# Инициализация Spark
spark = SparkSession.builder \
    .appName("S3ToChEarthquakes") \
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
    host=db_host,
    user=db_user,
    password=db_password,
    database="Denis_Chinese"
)

print(f"table_name === {table_name}")

cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS Denis_Chinese.{table_name} ON CLUSTER {cluster_name} (
        time_eq     DateTime,
        place       String,
        magnitude   Float64,
        felt        Nullable(Int32),
        tsunami     Nullable(Int32),
        load_date   DateTime,         -- дата выгрузки с портала погоды
        updated_at  DateTime          -- дата загрузки в CH
    )
    ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(load_date)
    ORDER BY (load_date, time_eq)
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS Denis_Chinese.{distributed_table_name}
    ENGINE = Distributed('{cluster_name}', 'Denis_Chinese', '{table_name}', rand())
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
    prefix="Denis_Chinese/Earthquakes",
    update_at=max_updated
)

if not new_files:
    print("Нет новых файлов для обработки")
    spark.stop()
    exit(0)

print(f"Найдено {len(new_files)} новых файлов для обработки")


# Читаем данные
df = spark.read.parquet(*new_files)

# Преобразование и обогащение
flattened = (
    df.select(
        date_format((col("time") / 1000).cast("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("time_eq"),        
        col("place").alias("place"),
        col("mag").alias("magnitude"),
        col("felt").alias("felt"),
        col("tsunami").alias("tsunami"),
        date_format((col("updated") / 1000).cast("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("load_date")
    )
    .withColumn("updated_at", 
                date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
)

# Запись в ClickHouse
flattened.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("dbtable", table_name) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("dbtable", distributed_table_name) \
    .mode("append") \
    .save()

print(f"Кол-во срок = {flattened.count()}")

print("✅ Данные успешно загружены в ClickHouse.")

