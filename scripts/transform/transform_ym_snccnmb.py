from pyspark.sql import SparkSession
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
s3_path_manifest = os.getenv('S3_PATH_MANIFEST')
s3_path_yandex_metrika = os.getenv("S3_PATH_YANDEX_METRIKA")


# Инициализация Spark
spark = (
    SparkSession.builder.appName("S3CHYM")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)

# Инициализация S3 менеджера
s3_manager = S3FileManager(
    bucket_name=os.getenv("MINIO_PROD_BUCKET_NAME"),
    aws_access_key=os.getenv("MINIO_ROOT_USER"),
    aws_secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    endpoint_url="http://minio:9000",
)

database = "snccnmb"

# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=db_host, user=db_user, password=db_password, database=database
)

print(f"table_name ===== {table_name}")

cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS {database}.{table_name} ON CLUSTER '{{cluster}}' (
        date         		 Date,
        gender  	 		 String,
        visits       		 Decimal(7, 1),
        users		 		 Decimal(7, 1),
        avgdaysbetweenvisits Decimal(7, 1),
        robotvisits 		 Decimal(7, 1),
        blockedpercentage 	 Decimal(7, 1),
        updated_at   		 DateTime64(0)
    )
    ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(date)
    ORDER BY (date, gender)
    """


create_distributed = f"""
    CREATE TABLE IF NOT EXISTS {database}.{distributed_table_name}
    AS {database}.{table_name}
    ENGINE = Distributed('{cluster_name}', '{database}', '{table_name}', halfMD5(date));
    """

print(create_main)
print(create_distributed)

ch_manager.execute_sql(create_main)
ch_manager.execute_sql(create_distributed)

print("Таблицы созданы")
print("Чтение манифеста через Spark...")
manifest_df = spark.read.text(s3_path_manifest)  # s3a путь напрямую в Spark
file_paths = [row["value"].strip() for row in manifest_df.collect() if row["value"].strip()]
print(f"Найдено файлов: {len(file_paths)}")

for i in range(0, len(file_paths), 10):
    batch = file_paths[i:i+10]
    batch_paths = [f"s3a://dev/{path}" for path in batch]  # Добавляем s3a://dev/
    print(f"Загрузка батча {i//10 + 1}, файлов: {len(batch_paths)}")
    
    df = spark.read.parquet(*batch_paths)

    renamed_df = (
        df
        .withColumnRenamed("ym:s:date", "date")
        .withColumnRenamed("ym:s:gender", "gender")
        .withColumnRenamed("ym:s:visits", "visits")
        .withColumnRenamed("ym:s:users", "users")
        .withColumnRenamed("ym:s:avgDaysBetweenVisits", "avgdaysbetweenvisits")
        .withColumnRenamed("ym:s:robotVisits", "robotvisits")
        .withColumnRenamed("ym:s:blockedPercentage", "blockedpercentage")
        .withColumnRenamed("update_at", "updated_at")
        )

    # Запись в ClickHouse
    (
        renamed_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", distributed_table_name)
        .mode("append")
        .save()
    )

    print(f"Кол-во срок == {renamed_df.count()}")

    print("✅ Батч успешно загружен в ClickHouse.")
