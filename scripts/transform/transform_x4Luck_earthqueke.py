import os
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    DateType,
    DoubleType,
    LongType,
    IntegerType,
    FloatType,
)
from s3_file_manager import S3FileManager
from clickhouse_manager import ClickHouseManager

jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
db_host = os.getenv("CLICKHOUSE_HOST")
db_user = os.getenv("CLICKHOUSE_USER")
db_password = os.getenv("CLICKHOUSE_PASSWORD")
db_name = os.getenv("DB_NAME")  # x4Luck
s3_path_earthquake = os.getenv("S3_PATH_EARTHQUAKE")
s3_path_manifest = f"{os.getenv('S3_PATH_MANIFEST')}"
table_name = f"{os.getenv('TABLE_NAME')}_local"
distributed_table_name = os.getenv("TABLE_NAME")


# Инициализация Spark
spark = (
    SparkSession.builder.appName("S3ToChEarthquakes")
    .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)

# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=db_host, user=db_user, password=db_password, database=db_name
)

# Инициализация S3 менеджера
s3_manager = S3FileManager(
    bucket_name=os.getenv("MINIO_DEV_BUCKET_NAME"),
    aws_access_key=os.getenv("MINIO_ROOT_USER"),
    aws_secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    endpoint_url="http://minio:9000",
)

cluster_name = "company_cluster"

sql_create_main_table = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ON CLUSTER {cluster_name}(
        mag Float32,
        place String,
        tz String,
        url String,
        detail String,
        felt Int8,
        cdi Float64,
        mmi Float64,
        alert String,
        status String,
        tsunami Int8,
        sig Int8,
        net String,
        code String,
        ids String,
        sources String,
        types String,
        nst Int8,
        dmin Float64,
        rms Float64,
        gap Int16,
        magType String,
        type String,
        title String,
        update_at Date,
        time DateTime,
        updated DateTime,
        business_date Date
    )
    ENGINE = ReplacingMergeTree(update_at)
    PARTITION BY toYYYYMM(business_date)
    ORDER BY (time)

"""

sql_create_distrubuted_table = f"""
CREATE TABLE IF NOT EXISTS {db_name}.{distributed_table_name} ON CLUSTER {cluster_name}
AS {db_name}.{table_name}
ENGINE = Distributed({cluster_name}, {db_name}, {table_name}, rand())
"""

ch_manager.execute_sql(sql_create_main_table)
ch_manager.execute_sql(sql_create_distrubuted_table)
print("Таблицы в CH созданы")


# Трансформаиця на Spark
def batch_generator_from_lines(lines, batch_size=10):
    batch = []
    for line in lines:
        if line:
            batch.append(line)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


print(f"Путь до манифеста: {s3_path_manifest}")
lines = s3_manager.stream_lines_from_s3(s3_path_manifest)

for batch_paths in batch_generator_from_lines(lines, 10):
    print("Загрузка в ClickHouse батчами по 10 файлов")
    df = spark.read.parquet(*batch_paths)

    result_df = (
        df.select(
            f.col("mag").cast(FloatType()).alias("mag"),
            f.col("place").cast(StringType()).alias("place"),
            f.col("time").cast(LongType()).alias("time"),
            f.col("updated").cast(LongType()).alias("updated"),
            f.col("tz").cast(StringType()).alias("tz"),
            f.col("url").cast(StringType()).alias("url"),
            f.col("detail").cast(StringType()).alias("detail"),
            f.col("felt").cast(IntegerType()).alias("felt"),
            f.col("cdi").cast(DoubleType()).alias("cdi"),
            f.col("mmi").cast(DoubleType()).alias("mmi"),
            f.col("alert").cast(StringType()).alias("alert"),
            f.col("status").cast(StringType()).alias("status"),
            f.col("tsunami").cast(IntegerType()).alias("tsunami"),
            f.col("sig").cast(IntegerType()).alias("sig"),
            f.col("net").cast(StringType()).alias("net"),
            f.col("code").cast(StringType()).alias("code"),
            f.col("ids").cast(StringType()).alias("ids"),
            f.col("sources").cast(StringType()).alias("sources"),
            f.col("types").cast(StringType()).alias("types"),
            f.col("nst").cast(IntegerType()).alias("nst"),
            f.col("dmin").cast(DoubleType()).alias("dmin"),
            f.col("rms").cast(DoubleType()).alias("rms"),
            f.col("gap").cast(IntegerType()).alias("gap"),
            f.col("magType").cast(StringType()).alias("magType"),
            f.col("type").cast(StringType()).alias("type"),
            f.col("title").cast(StringType()).alias("title"),
            f.col("update_at").cast(DateType()).alias("update_at"),
        )
        .withColumn(
            "time",
            f.from_unixtime((f.col("time") / 1000).cast("long")).cast("timestamp"),
        )
        .withColumn(
            "updated",
            f.from_unixtime((f.col("updated") / 1000).cast("long")).cast("timestamp"),
        )
        .withColumn("business_date", f.to_date(f.col("time")))
        # ✅ критично: убираем дроби в тексте
        .withColumn("time", f.date_format(f.col("time"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("updated", f.date_format(f.col("updated"), "yyyy-MM-dd HH:mm:ss"))
    )

    (
        result_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("database", db_name)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", distributed_table_name)
        .mode("append")
        .save()
    )
    print("✅ Батч успешно загружен в ClickHouse.")
