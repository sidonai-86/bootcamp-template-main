from pyspark.sql import SparkSession
from s3_file_manager_2 import S3FileManager
import pyspark.sql.functions as F
# from clickhouse_manager import ClickHouseManager
import os
import logging
import pendulum
import clickhouse_connect as cc
from spark_to_ch_dtypes_prepare import SparkClickHouseDtypesPrepare


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__) 

jdbc_url = os.getenv('CLICKHOUSE_JDBC_URL')
db_host = os.getenv('CLICKHOUSE_HOST')
db_user = os.getenv('CLICKHOUSE_USER')
db = os.getenv('DB')
db_password = os.getenv('CLICKHOUSE_PASSWORD')
table_name = f"{os.getenv('TABLE_NAME')}_local"
s3_path_earthquake = os.getenv('S3_PATH_EARTHQUAKE')
distributed_table_name = os.getenv('TABLE_NAME')
cluster_name = os.getenv('CLUSTER_NAME')
execution_date_str = os.getenv('EXECUTION_DATE')
EXECUTION_DATE = pendulum.parse(execution_date_str).to_date_string()
logger.info(f"{EXECUTION_DATE=}")
logger.info(f"{s3_path_earthquake=}")

# Инициализация Spark
spark = SparkSession.builder \
    .appName("S3ToChEarthquake") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()


ch_client = cc.get_client(
    host=db_host,
    port="8123",
    username=db_user,
    password=db_password,
    database=db
)

create_main = f"""
    CREATE TABLE IF NOT EXISTS {db}.{table_name} ON CLUSTER '{cluster_name}'
    (
        date Date,
        time_min Datetime64(3),
        time_max Datetime64(3),
        latitude Float64,
        longitude Float64,
        depth Float64,
        mag Float64,
        magType LowCardinality(String),
        nst Int32,
        gap Int32,
        dmin Float64,
        rms Float64,
        net LowCardinality(String),
        updated_max Datetime64(3),
        place_count UInt16,
        type LowCardinality(String),
        horizontalError Float64,
        depthError Float64,
        magError Float64,
        magNst Int32,
        status LowCardinality(String),
        locationSource LowCardinality(String),
        magSource LowCardinality(String),
        updated_at Datetime
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster_name}/{{{db}}}/{{shard}}/earthquake_events_local', '{{replica}}')
    PARTITION BY date
    ORDER BY (date, id)
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS {db}.{distributed_table_name}
    ENGINE = Distributed('{cluster_name}', '{db}', '{table_name}', rand())
    """

# logger.info(create_main)
# logger.info(create_distributed)

ch_client.command(create_main)
ch_client.command(create_distributed)

logger.info("Таблицы созданы")

filename = f"events_{EXECUTION_DATE}.parquet"

# Читаем данные
df = spark.read.parquet(s3_path_earthquake+filename)

logger.info(f"{df.count()=}")
# logger.info(f"{df.schema=}")

s3_manager = S3FileManager(
    bucket_name=os.getenv("MINIO_PROD_BUCKET_NAME"),
    aws_access_key=os.getenv("MINIO_ROOT_USER"),
    aws_secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    endpoint_url="http://minio:9000",
)


df_agg = (df.withColumn("date", F.to_date("time"))
    .groupBy(["date","magType", "net", "type", "status", "locationSource", "magSource"])
    .agg(
        F.min("time").alias("time_min"),
        F.max("time").alias("time_max"),
        F.avg("latitude").alias("latitude"),
        F.avg("longitude").alias("longitude"),
        F.avg("depth").alias("depth"),
        F.avg("mag").alias("mag"),
        F.avg("nst").alias("nst"),
        F.avg("gap").alias("gap"),
        F.avg("dmin").alias("dmin"),
        F.avg("rms").alias("rms"),
        F.max("updated").alias("updated_max"),
        F.count_distinct("place").alias("place_count"),
        F.avg("horizontalError").alias("horizontalError"),
        F.avg("depthError").alias("depthError"),
        F.avg("magError").alias("magError"),
        F.avg("magNst").alias("magNst"),
    ).withColumn("updated_at", F.current_timestamp())
)

need_cols = (
    list(ch_client.query_df(f"""SELECT name, position FROM system.columns where table = '{distributed_table_name}' """)
    .sort_values("position")
    .name
    .values)
)

df_preparer = SparkClickHouseDtypesPrepare(
    simple_columns = {
    "date": "date",
    "time_min": "datetime64_3",
    "time_max": "datetime64_3",
    "latitude": "float64",
    "longitude": "float64",
    "depth": "float64",
    "mag": "float64",
    "magType": "string",
    "nst": "int32",
    "gap": "int32",
    "dmin": "float64",
    "rms": "float64",
    "net": "string",
    "updated_max": "datetime64_3",
    "place_count": "uint16",
    "type": "string",
    "horizontalError": "float64",
    "depthError": "float64",
    "magError": "float64",
    "magNst": "int32",
    "status": "string",
    "locationSource": "string",
    "magSource": "string",
    "updated_at": "datetime"
    },
    need_cols = need_cols 
)
prepared_df = df_preparer.prepare(df_agg)

ch_client.command(f"""alter table {table_name} on cluster '{cluster_name}' drop partition '{EXECUTION_DATE}' """)

prepared_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("database", db) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("dbtable", distributed_table_name) \
    .mode("append") \
    .save()


print(f"Кол-во срок == {prepared_df.count()}")

print("✅ Данные успешно загружены в ClickHouse.")
