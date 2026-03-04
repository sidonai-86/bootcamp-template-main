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
s3_path_regions = os.getenv('S3_PATH_REGIONS')
s3_path_earthquake = os.getenv('S3_PATH_EARTHQUAKE')
distributed_table_name = os.getenv('TABLE_NAME')
exec_date = os.getenv("EXEC_DATE")


# Инициализация Spark
spark = SparkSession.builder \
    .appName("goggle_mogle_s3_ch") \
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
    database="default"
)

parquet_path = f"s3a://dev/goggle_mogle/earthquake/{exec_date}.parquet"
df = spark.read.parquet(parquet_path)

final = (
    df.selectExpr("explode(features) as feature") \
    .select(
        col("feature.id").alias("id"),
        col("feature.properties.mag").alias("magnitude"),
        trim(split(col("feature.properties.place"), ",").getItem(0)).alias("place"),
        trim(split(col("feature.properties.place"), ",").getItem(1)).alias("initial_region"),
        from_unixtime(col("feature.properties.time") / 1000).cast("date").alias("feature_date"),
        col("feature.properties.felt").alias("felt"),
        col("feature.properties.cdi").alias("cdi"),
        col("feature.properties.mmi").alias("mmi"),
        col("feature.properties.status").alias("status"),
        col("feature.properties.tsunami").alias("tsunami"),
        col("feature.properties.sig").alias("sig"),
        col("feature.properties.nst").alias("nst"),
        col("feature.properties.dmin").alias("dmin"),
        col("feature.properties.rms").alias("rms"),
        col("feature.properties.gap").alias("gap"),
        col("feature.properties.magType").alias("magType"),
        col("feature.geometry.coordinates")[0].alias("longitude"),
        col("feature.geometry.coordinates")[1].alias("latitude"),
        col("feature.geometry.coordinates")[2].alias("depth"),
    )
    .withColumn("updated_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
)

print(f'Количество строк: {final.count()}')

cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS goggle_mogle.{table_name} ON CLUSTER {cluster_name} (
        id String,
        magnitude Float64,
        place String,
        initial_region String,
        feature_date Date,
        felt Int32,
        cdi Float64,
        mmi Float64,
        status String,
        tsunami UInt8,
        sig Int32,
        nst Int32,
        dmin Float64,
        rms Float64,
        gap Int32,
        magType String,
        longitude Float64,
        latitude Float64,
        depth Float64,
        updated_at DateTime
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(feature_date)
    ORDER BY (cityHash64(id));
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS goggle_mogle.{distributed_table_name} ON CLUSTER {cluster_name}
    AS goggle_mogle.{table_name}
    ENGINE = Distributed(
        '{cluster_name}',
        'goggle_mogle',
        '{table_name}',
        cityHash64(id)
    );
    """

ch_manager.execute_sql(create_main)
ch_manager.execute_sql(create_distributed)

final.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("dbtable", distributed_table_name) \
    .mode("append") \
    .save()

print('Успех')