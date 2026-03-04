from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    arrays_zip,
    unix_timestamp,
    from_unixtime,
    date_format,
    to_date, 
    regexp_replace
)
from clickhouse_manager import ClickHouseManager
import os

# ⬇️ Параметры подключения к CLICKHOUSE
ch_jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
ch_host = os.getenv("CLICKHOUSE_HOST")
ch_user = os.getenv("CLICKHOUSE_USER")
ch_password = os.getenv("CLICKHOUSE_PASSWORD")
ch_table_name = f"{os.getenv('CLICKHOUSE_TABLE_NAME')}_local"
ch_distributed_table_name = os.getenv("CLICKHOUSE_TABLE_NAME")
ch_cluster_name = os.getenv("CLICKHOUSE_CLUSTER_NAME")
ch_database = os.getenv("CLICKHOUSE_DATABASE")

# Инициализация Spark
spark = (
    SparkSession.builder.appName("GPToCh")
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
        "org.postgresql:postgresql:42.5.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        )
    .getOrCreate()
)

# ⬇️ Параметры подключения к PostgreSQL 
pg_jdbc_url = os.getenv("PG_JDBC_URL")
pg_user = os.getenv("PG_USER") 
pg_password = os.getenv("PG_PASSWORD") 
pg_table_name_shops = os.getenv("PG_TABLE_NAME_SHOPS")
pg_table_name_shops_tz = os.getenv("PG_TABLE_NAME_SHOPS_TZ")


shops_df = (spark.read
            .format("jdbc")
            .option("url", pg_jdbc_url)
            .option("user", pg_user)
            .option("password", pg_password)
            .option("dbtable", pg_table_name_shops)
            .option("fetchsize", 1000)
            .option("driver", "org.postgresql.Driver")
            .load()
            )


shop_tz_df = (spark.read
				.format("jdbc")
				.option("url", pg_jdbc_url)
				.option("user", pg_user)
				.option("password", pg_password)
				.option("dbtable", pg_table_name_shops_tz) 
				.option("fetchsize", 1000)
				.option("driver", "org.postgresql.Driver")
                .load()
)

shops_df.createOrReplaceTempView("shops") 
shop_tz_df.createOrReplaceTempView("shop_tz")

# Выполняем SQL запрос для трансформации 
df_transform = spark.sql("""        
                        WITH sz AS (
                        SELECT  TRY_CAST(plant AS INT) AS id, 
                                coalesce(time_zone, '') AS time_zone
                        FROM shop_tz 
                        WHERE coalesce(TRY_CAST(plant AS INT), 0) > 0
                        ), 
                        s AS ( 
                        SELECT  TRY_CAST(st_id AS INT) AS st_id, 
                                shop_name 
                        FROM shops
                        ), 
                        result AS (
                        SELECT  *, 
                                ROW_NUMBER() over (PARTITION BY st_id ORDER BY time_zone DESC) AS rn
                        FROM s 
                        JOIN sz 
                                ON s.st_id = sz.id ) 
                        SELECT  st_id, 
                        shop_name, 
                        TRY_CAST(CASE WHEN time_zone = '' THEN 3 ELSE substr(time_zone, 4) END AS INT) AS tz_code 
                        FROM result 
                        WHERE rn = 1 
                """
)


type_mapping = {
    "st_id": "long",
    "shop_name": "string", 
    "tz_code": "byte"
}

df_final = df_transform.select([
    col(c).cast(t).alias(c) for c, t in type_mapping.items()
])

# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=ch_host, user=ch_user, password=ch_password, database=ch_database
)

drop_local =  f"""
    DROP TABLE IF EXISTS {ch_database}.{ch_table_name} ON CLUSTER {ch_cluster_name} SYNC
"""
create_local = f"""
    CREATE TABLE IF NOT EXISTS {ch_database}.{ch_table_name} ON CLUSTER {ch_cluster_name}
    (
        st_id           UInt64                 COMMENT 'id магзина',
        shop_name       String                 COMMENT 'Наименование магазина',
        tz_code         UInt8                  COMMENT 'Часовой пояс'
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/avpalatov_{ch_table_name}', '{{replica}}')
    ORDER BY (st_id)
    COMMENT 'Таблица магазинов с часовыми поясами'
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS {ch_database}.{ch_distributed_table_name}
    AS {ch_database}.{ch_table_name}
    ENGINE = Distributed('{ch_cluster_name}', '{ch_database}', '{ch_table_name}', st_id);
    """

print(drop_local)
print(create_local)
print(create_distributed)

ch_manager.execute_sql(drop_local)
ch_manager.execute_sql(create_local)
ch_manager.execute_sql(create_distributed)


df_final.write \
    .format("jdbc") \
    .option("url", ch_jdbc_url) \
    .option("user", ch_user) \
    .option("password", ch_password) \
    .option("dbtable", ch_distributed_table_name) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("truncate", "true") \
    .mode("append") \
    .save()


print(f"Кол-во срок == {df_final.count()}")

print("✅ Данные успешно загружены в ClickHouse.")
