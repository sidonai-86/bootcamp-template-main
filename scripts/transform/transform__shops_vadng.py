from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, coalesce, lit
from pyspark.sql.types import IntegerType
from clickhouse_manager import ClickHouseManager
import os


jdbc_url_pg = "jdbc:postgresql://postgres_source:5432/source"
db_user_pg = os.getenv("POSTGRES_USER")
db_password_pg = os.getenv("POSTGRES_PASSWORD")
jdbc_url_ch = os.getenv("CLICKHOUSE_JDBC_URL")
db_host_ch = os.getenv("CLICKHOUSE_HOST")
db_user_ch = os.getenv("CLICKHOUSE_USER")
db_password_ch = os.getenv("CLICKHOUSE_PASSWORD")
table_name = f"{os.getenv('TABLE_NAME')}_local"
distributed_table_name = os.getenv("TABLE_NAME")


# Инициализация Spark
spark = (
    SparkSession.builder.appName("S3ToChEarthquake")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)


# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=db_host_ch, user=db_user_ch, password=db_password_ch, database="vadng"
)


cluster_name = "company_cluster"

create_main = f"""
    CREATE TABLE IF NOT EXISTS vadng.{table_name} ON CLUSTER {cluster_name} (
        st_id UInt16,
        shop_name String,
        tz_code Uint8
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{cluster}}/{{shard}}/{table_name}','{{replica}}')
    ORDER BY (st_id)
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS default.{distributed_table_name} ON CLUSTER {cluster_name}
    AS vadng.{table_name}
    ENGINE = Distributed('{cluster_name}', 'vadng', '{table_name}', rand())
    """

print(create_main)
print(create_distributed)

ch_manager.execute_sql(create_main)
ch_manager.execute_sql(create_distributed)

shops_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url_pg) \
            .option("user", db_user_pg) \
            .option("password", db_password_pg) \
            .option("dbtable", "public.shops") \
            .option("fetchsize", 1000) \
            .option("driver", "org.postgresql.Driver") \
            .load()

shop_tz_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url_pg) \
            .option("user", db_user_pg) \
            .option("password", db_password_pg) \
            .option("dbtable", "public.shop_timezone") \
            .option("fetchsize", 1000) \
            .option("driver", "org.postgresql.Driver") \
            .load()

shops_df.createOrReplaceTempView("shops")
shop_tz_df.createOrReplaceTempView("shop_timezone")

timezones = (
            shop_tz_df
            .where("""TRY_CAST(plant AS BIGINT) IS NOT NULL AND time_zone != '' """)
            .select("plant", "time_zone")
            .distinct()
            .alias('t')
)

final = (
        shops_df.alias('s')
        .join(timezones, col('s.st_id') == col('t.plant'), 'left')
        .select(
            's.st_id', 
            's.shop_name',
            coalesce(
                regexp_extract(col("t.time_zone"), r'[1-9]+', 0).cast(IntegerType()),
                lit(3)
            ).alias("tz_code")
     
        )
        .orderBy("s.st_id")
)

# Запись в ClickHouse
final.write \
    .format("jdbc") \
    .option("url", jdbc_url_ch) \
    .option("user", db_user_ch) \
    .option("password", db_password_ch) \
    .option("dbtable", table_name) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .mode("append") \
    .save()

print("✅ Данные успешно загружены в ClickHouse.")