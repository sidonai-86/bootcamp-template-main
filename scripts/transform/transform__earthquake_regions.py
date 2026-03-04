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


# Инициализация Spark
spark = SparkSession.builder \
    .appName("S3ToChEarthquakeRegions") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()


# Инициализация S3 менеджера
s3_manager = S3FileManager(
    bucket_name=os.getenv("MINIO_PROD_BUCKET_NAME"),
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

print("TEST")

cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS default.{table_name} ON CLUSTER {cluster_name} (
        id String,
        ts DateTime,
        place String,
        region String,
        magnitude Float64,
        felt Nullable(Int32),
        tsunami Nullable(Int32),
        url String,
        longitude Float64,
        latitude Float64,
        depth Float64,
        load_date Date,
        updated_at DateTime
    )
    ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(load_date)
    ORDER BY (load_date, id)
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS default.{distributed_table_name}
    ENGINE = Distributed('{cluster_name}', 'default', '{table_name}', rand())
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
    prefix="api/earthquake/",
    update_at=max_updated
)

print(f"Найдено {len(new_files)} новых файлов для обработки")

if not new_files:
    print("Нет новых файлов для обработки")
    spark.stop()
    exit(0)

# Читаем данные
df = spark.read.json(new_files)
regions = spark.read.parquet(s3_path_regions)

# Преобразование и обогащение
flattened = (df.selectExpr("explode(features) as feature") \
    .select(
        col("feature.id").alias("id"),
        from_unixtime(col("feature.properties.time") / 1000).alias("ts"),
        col("ts").cast("date").alias("load_date"),
        trim(split(col("feature.properties.place"), ",").getItem(0)).alias("place"),
        trim(split(col("feature.properties.place"), ",").getItem(1)).alias("initial_region"),
        col("feature.properties.mag").alias("magnitude"),
        col("feature.properties.felt").alias("felt"),
        col("feature.properties.tsunami").alias("tsunami"),
        col("feature.properties.url").alias("url"),
        col("feature.geometry.coordinates")[0].alias("longitude"),
        col("feature.geometry.coordinates")[1].alias("latitude"),
        col("feature.geometry.coordinates")[2].alias("depth"),
        md5(lower(trim(col("feature.properties.place")))).alias("place_hash")
    )
    .withColumn("updated_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")))


# Джоин с регионами
enriched = flattened.alias("f") \
                    .join(regions.alias("r"), on="place_hash", how="left") \
                    .select(
                        "id",
                        "ts",
                        "place",
                        coalesce(col("r.region"), col("initial_region")).alias("region"),
                        "magnitude",
                        "felt",
                        "tsunami",
                        "url",
                        "longitude",
                        "latitude",
                        "depth",
                        "load_date",
                        col("updated_at").cast("timestamp").alias("updated_at")
                    )

# Запись в ClickHouse
enriched.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("dbtable", table_name) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("dbtable", distributed_table_name) \
    .mode("append") \
    .save()

print(f"Кол-во срок == {enriched.count()}")

print("✅ Данные успешно загружены в ClickHouse.")
