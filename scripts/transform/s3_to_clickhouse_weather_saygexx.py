import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime

JDBC_URL = os.getenv("CLICKHOUSE_JDBC_URL")
TABLE_NAME = os.getenv("TABLE_NAME")
USER = os.getenv("CLICKHOUSE_USER")
PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
MANIFEST_KEY = os.getenv("S3_PATH_MANIFEST")

S3_BUCKET = "prod"

spark = (
    SparkSession.builder
    .appName("WeatherSnowIncremental_saygexx")
    .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    .getOrCreate()
)

print("=== Spark started ===")

manifest_path = f"s3a://{S3_BUCKET}/{MANIFEST_KEY}"
manifest_df = spark.read.text(manifest_path)
file_list = [row.value for row in manifest_df.collect()]

df = spark.read.parquet(*file_list)

print("=== ORIGINAL SCHEMA ===")
df.printSchema()

# 🔥 КОНВЕРТАЦИЯ НАНОСЕКУНД В TIMESTAMP
df_clean = (
    df
    .withColumn(
        "event_time",
        from_unixtime(col("event_time") / 1_000_000_000).cast("timestamp")
    )
    .withColumn("load_date", col("load_date").cast("date"))
    .withColumn("temperature_2m", col("temperature_2m").cast("double"))
    .withColumn("snowfall_mm", col("snowfall_mm").cast("double"))
)

df_clean = df_clean.filter(col("event_time").isNotNull())

print("=== CLEAN SCHEMA ===")
df_clean.printSchema()

print("Rows to insert:", df_clean.count())

(
    df_clean.write
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("user", USER)
    .option("password", PASSWORD)
    .option("dbtable", TABLE_NAME)
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .mode("append")
    .save()
)

print("=== SUCCESS ===")

spark.stop()