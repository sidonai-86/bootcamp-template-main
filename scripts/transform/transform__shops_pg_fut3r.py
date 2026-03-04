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
table_name = "st_timezone_local"
distributed_table_name = "st_timezone"
jdbc_url_pg = "jdbc:postgresql://postgres_source:5432/source" 
table_name_shops = "public.shops" 
table_name_tz = "public.shop_timezone" 
db_user_pg = os.getenv("POSTGRES_USER")
db_password_pg = os.getenv("POSTGRES_PASSWORD")


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



create_main = f"""
    CREATE TABLE IF NOT EXISTS fut3r.{table_name} ON CLUSTER {cluster_name} (
        st_id UInt32,
        shop_name String,
        tz_code UInt8
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{cluster_name}}/{{shard}}/fut3r_{table_name}', '{{replica}}')
    ORDER BY (st_id)
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS fut3r.{distributed_table_name} ON CLUSTER {cluster_name}
    ENGINE = Distributed('{cluster_name}', 'fut3r', '{table_name}', rand())
    """
truncate_local = f"""
    TRUNCATE TABLE IF EXISTS fut3r.{table_name} ON CLUSTER {cluster_name}
"""

print(create_main)
print(create_distributed)
print(truncate_local)

ch_manager.execute_sql(create_main)
ch_manager.execute_sql(create_distributed)
print("Таблицы созданы")
ch_manager.execute_sql(truncate_local)
print("Очищение таблицы выполнено")



# Читаем данные

shops_df = spark.read \
		.format("jdbc")\
		.option("url", jdbc_url_pg)\
		.option("user", db_user_pg)\
		.option("password", db_password_pg)\
		.option("dbtable", table_name_shops)\
		.option("fetchsize", 1000)\
		.option("driver", "org.postgresql.Driver")\
		.load() 

shop_timezone_df = spark.read \
		.format("jdbc")\
		.option("url", jdbc_url_pg)\
		.option("user", db_user)\
		.option("password", db_password)\
		.option("dbtable", table_name_tz)\
		.option("fetchsize", 1000)\
		.option("driver", "org.postgresql.Driver")\
		.load() 

tz_df = (
    shop_timezone_df.select(
        F.col("plant").cast("string"),
        F.coalesce(
            F.when(F.col("time_zone").rlike(r"RUS0\d+"), F.regexp_extract(F.col("time_zone"), r"RUS0(\d+)", 1)),
            F.lit(3)
         ).cast("int").alias('time_zone')
    )
)

res_df = (
    shops_df.join(tz_df, shops_df.st_id == tz_df.plant)
   .groupBy('st_id','shop_name')
   .agg(F.max('time_zone').alias('tz_code'))
   .show()
)




# Запись в ClickHouse
res_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("dbtable", distributed_table_name) \
    .mode("append") \
    .save()

print(f"Кол-во загружаемых строк == {res_df.count()}")

print("✅ Данные успешно загружены в ClickHouse.")