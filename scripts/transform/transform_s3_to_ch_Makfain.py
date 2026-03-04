# запускаем спарк 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip, unix_timestamp, from_unixtime, current_timestamp
from clickhouse_manager import ClickHouseManager
from s3_file_manager import S3FileManager
from typing import Iterator, List


spark = SparkSession.builder \
    .appName("SparkExample") \
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
        "org.postgresql:postgresql:42.5.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        ) \
    .getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.path.style.access", "true")

jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
db_host = os.getenv("CLICKHOUSE_HOST")
db_user = os.getenv("CLICKHOUSE_USER")
db_password = os.getenv("CLICKHOUSE_PASSWORD")
table_name = f"{os.getenv('TABLE_NAME')}_local"
distributed_table_name = f"{os.getenv('TABLE_NAME')}_dis"
s3_path_manifest = os.getenv("S3_PATH_MANIFEST")

# Инициализация S3 менеджера
s3_manager = S3FileManager(
    bucket_name="dev",
    aws_access_key=os.getenv("MINIO_ROOT_USER"),
    aws_secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    endpoint_url="http://minio:9000",
)

print("BUCKET =", s3_manager.bucket_name)
print("MANIFEST KEY =", s3_path_manifest)

# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=db_host, user=db_user, password=db_password, database="Makfain"
)

# Создание таблиц в ClickHouse
cluster_name = "company_cluster"
create_main = f"""
    CREATE TABLE IF NOT EXISTS Makfain.{table_name} ON CLUSTER {cluster_name} (
        title String,
        user_login String,
        created_at DateTime64(0),
        url String, 
        updated_at DateTime64(0)
    )
    ENGINE = ReplacingMergeTree(created_at )
    PARTITION BY toYYYYMM(toDateTime(created_at))
    ORDER BY (user_login, created_at)
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS Makfain.{distributed_table_name}
    AS Makfain.{table_name}
    ENGINE = Distributed('{cluster_name}', 'Makfain', '{table_name}', halfMD5(updated_at));
    """

# Выполнение SQL для создания таблиц
ch_manager.execute_sql(create_main)
ch_manager.execute_sql(create_distributed)

# Чтение путей из new_files.txt
def read_file_paths_from_s3(s3_path):
    lines = s3_manager.stream_lines_from_s3(s3_path)
    return [line.strip() for line in lines if line.strip()]

# Получаем список путей к файлам Parquet
raw_paths = read_file_paths_from_s3(s3_path_manifest)

file_paths = [line.strip() for line in raw_paths if line.strip()]

# Трансформация данных и загрузка в ClickHouse батчами
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

for batch_paths in batch_generator_from_lines(file_paths, 10):
    print("Загрузка в ClickHouse батчами по 10 файлов")
    
    # Чтение Parquet файлов
    df = spark.read.parquet(*batch_paths)

    
    # Преобразование данных в нужные форматы
    parsed_df = (
        df
        .withColumn("updated_at", current_timestamp())
        .select("title", "user_login", "created_at", "url", "updated_at")
    )

    # Запись данных в ClickHouse
    parsed_df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("dbtable", distributed_table_name) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

    print(f"Кол-во строк == {parsed_df.count()}")
    print("✅ Батч успешно загружен в ClickHouse.")
