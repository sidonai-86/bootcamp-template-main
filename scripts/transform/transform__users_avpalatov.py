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

jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
db_host = os.getenv("CLICKHOUSE_HOST")
db_user = os.getenv("CLICKHOUSE_USER")
db_password = os.getenv("CLICKHOUSE_PASSWORD")
table_name = f"{os.getenv('TABLE_NAME')}_local"
distributed_table_name = os.getenv("TABLE_NAME")
s3_path = os.getenv('S3_PATH')
cluster_name = "company_cluster"
ch_database = "avpalatov"

# Инициализация Spark
spark = (
    SparkSession.builder.appName("CVSToCh")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)

df = spark.read.csv(s3_path, header=True, sep=',')

df_transform = (df
    .withColumnRenamed("tg_id", "telegram_id")
    .withColumn("user_nickname", regexp_replace(col("tg_nickname"), r"^@", ""))  # Убираем @
    .withColumn("registration_date", to_date(col("update_at")))  # TS → DATE
    .drop("pk_id","tg_nickname","update_at")
)

type_mapping = {
    "telegram_id": "long",
    "user_nickname": "string", 
    "registration_date": "date"
}

df_final = df_transform.select([
    col(c).cast(t).alias(c) for c, t in type_mapping.items()
])

# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=db_host, user=db_user, password=db_password, database=ch_database
)

drop_local =  f"""
    DROP TABLE IF EXISTS {ch_database}.{table_name} ON CLUSTER {cluster_name} SYNC
"""
create_local = f"""
    CREATE TABLE IF NOT EXISTS {ch_database}.{table_name} ON CLUSTER {cluster_name}
    (
        telegram_id         UInt64                 COMMENT 'id telegram',
        user_nickname       String                 COMMENT 'Никнейм',
        registration_date   Date                   COMMENT 'Дата регистрации'
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/avpalatov_{table_name}', '{{replica}}')
    ORDER BY (telegram_id)
    COMMENT 'Регистрации кто хочет на буткемп'
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS {ch_database}.{distributed_table_name}
    AS {ch_database}.{table_name}
    ENGINE = Distributed('{cluster_name}', '{ch_database}', '{table_name}', telegram_id);
    """

print(drop_local)
print(create_local)
print(create_distributed)

ch_manager.execute_sql(drop_local)
ch_manager.execute_sql(create_local)
ch_manager.execute_sql(create_distributed)



df_final.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("dbtable", distributed_table_name) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("truncate", "true") \
    .mode("append") \
    .save()


print(f"Кол-во срок == {df_final.count()}")

print("✅ Файлы успешно загружены в ClickHouse.")
