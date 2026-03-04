from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, current_timestamp, to_date
from s3_file_manager import S3FileManager
from clickhouse_manager import ClickHouseManager
import os

jdbc_url = os.getenv('CLICKHOUSE_JDBC_URL')
db_host = os.getenv('CLICKHOUSE_HOST')
db_user = os.getenv('CLICKHOUSE_USER')
db_password = os.getenv('CLICKHOUSE_PASSWORD')
table_name = f"{os.getenv('TABLE_NAME')}_local"
distributed_table_name = os.getenv('TABLE_NAME')
s3_path_earthquake = os.getenv('S3_PATH_WEATHER')

# Инициализация Spark
spark = SparkSession.builder \
    .appName("S3ToChWeather") \
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
    database="vvv_db"
)

print("TESTTESTTEST TESTTESTTESTTEST")

cluster_name = "company_cluster"

# Создание локальной таблицы
create_main = f"""
CREATE TABLE IF NOT EXISTS vvv_db.{table_name} ON CLUSTER {cluster_name} (
    time String,
    temperature_2m Float32,
    rain Float32,
    updated_at DateTime64(0),
    load_date Date
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(load_date)
ORDER BY (time)
"""

# Создание распределенной таблицы 
create_distributed = f"""
CREATE TABLE IF NOT EXISTS vvv_db.{distributed_table_name} 
AS vvv_db.{table_name}
ENGINE = Distributed('{cluster_name}', 'vvv_db', '{table_name}', halfMD5(time));
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
    prefix="vvv_Nika/API_to_S3/",
    update_at=max_updated
)

print(f"Найдено {len(new_files)} новых файлов для обработки")

if not new_files:
    print("Нет новых файлов для обработки")
    spark.stop()
    exit(0)

# Читаем данные
df = spark.read.parquet(*new_files)


# Преобразование данных
final_df = df.select(
    col("time"),
    col("temperature_2m"),
    col("rain")
).withColumn("updated_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
 .withColumn("load_date", to_date(col("time")))


# Запись в ClickHouse
final_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("dbtable", table_name) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("dbtable", distributed_table_name) \
    .mode("append") \
    .save()

print(f"Кол-во срок == {final_df.count()}")

print("✅ Данные успешно загружены в ClickHouse.")