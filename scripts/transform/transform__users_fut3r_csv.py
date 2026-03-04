from pyspark.sql import SparkSession
from pyspark import SparkFiles
import pyspark.sql.functions as F
from s3_file_manager import S3FileManager
from clickhouse_manager import ClickHouseManager
from datetime import datetime, timedelta, timezone
import os


jdbc_url = os.getenv('CLICKHOUSE_JDBC_URL')
db_host = os.getenv('CLICKHOUSE_HOST')
db_user = os.getenv('CLICKHOUSE_USER')
db_password = os.getenv('CLICKHOUSE_PASSWORD')
table_name = "users_local2"
distributed_table_name = "users2"


# Инициализация Spark
spark = SparkSession.builder \
    .appName("CSVTransform") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()


# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=db_host,
    user=db_user,
    password=db_password,
    database="fut3r"
)

cluster_name = "company_cluster"

truncate_local = f"""
    TRUNCATE TABLE fut3r.{table_name} ON CLUSTER {cluster_name}
"""

create_main = f"""
    CREATE TABLE IF NOT EXISTS fut3r.{table_name} ON CLUSTER {cluster_name} (
        telegram_id String,
        user_nickname String,
        registration_date Date
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{cluster_name}}/{{shard}}/fut3r_{table_name}', '{{replica}}')
    ORDER BY (telegram_id)
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS fut3r.{distributed_table_name}
    ENGINE = Distributed('{cluster_name}', 'fut3r', '{table_name}', rand())
    """
print(truncate_local)
print(create_main)
print(create_distributed)

ch_manager.execute_sql(truncate_local)
ch_manager.execute_sql(create_main)
ch_manager.execute_sql(create_distributed)

print("Таблицы созданы")

filenames = ["google_users.csv", "other_users.csv", "yandex_users.csv"]

dfs = []

for name in filenames:
    local_path = SparkFiles.get(name)
    print(f"Файл {name} доступен по пути: {local_path}")
    
    df = spark.read.csv(local_path, header=True, inferSchema=True)
    dfs.append(df)

final_df = dfs[0].unionAll(dfs[1]).unionAll(dfs[2])



# Читаем данные
#df = spark.read.parquet(*new_files)
#regions = spark.read.parquet(s3_path_regions)

df_transf = final_df.select(
    F.regexp_replace(F.col("tg_id"), "@", "").alias("telegram_id"),
    F.regexp_replace(F.col("tg_nickname"), "@", "").alias("user_nickname"),
    F.col("update_at").cast("date").alias("registration_date") 
).distinct()

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