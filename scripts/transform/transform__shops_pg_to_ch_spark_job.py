import clickhouse_connect
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, substring, max as spark_max, coalesce, lit
import os
from pyspark.sql import functions as F
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

spark = SparkSession.builder \
    .appName("SparkExample") \
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
        "org.postgresql:postgresql:42.5.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        ) \
    .getOrCreate()

logging.info(f"Активные Spark сессии: {spark.sparkContext.uiWebUrl}")

jdbc_url = "jdbc:postgresql://postgres_source:5432/source"
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")


df_shops = (spark
            .read
            .format("jdbc") 
            .option("url", jdbc_url) 
            .option("user", db_user) 
            .option("password", db_password) 
            .option("dbtable", "public.shops") 
            .option("fetchsize", 1000) 
            .option("driver", "org.postgresql.Driver") 
            .load()
)

logging.info(f"Структура таблицы shops: {df_shops.printSchema()}")

df_shop_timezone = (spark
            .read
            .format("jdbc") 
            .option("url", jdbc_url) 
            .option("user", db_user) 
            .option("password", db_password) 
            .option("dbtable", "public.shop_timezone") 
            .option("fetchsize", 1000) 
            .option("driver", "org.postgresql.Driver") 
            .load()
)

logging.info(f"Структура таблицы shop_timezone: {df_shop_timezone.printSchema()}")


result_df = (df_shop_timezone
    .filter(col("plant").like("8%"))
    .join(df_shops, 
          df_shops["st_id"].cast("string") == df_shop_timezone["plant"], 
          "left")
    .groupBy("plant", "shop_name")
    .agg(
        coalesce(
            spark_max(
                when(col("time_zone").like("RUS0%"), 
                     substring(col("time_zone"), 5, 1)
                )
            ),
            lit("3")
        ).alias("tz_code")
    )
    .select(
        col("plant").cast("int").alias("st_id"),
        col("shop_name"),
        col("tz_code")
    )
)

#result_df.show()

logging.info(f"Структура итоговой таблицы: {result_df.printSchema()}")


# ⬇️ Параметры подключения к CLICKHOUSE

jdbc_url = 'jdbc:clickhouse://clickhouse01:8123/vlad_meniailov'
host = 'clickhouse01'
db_user = os.getenv('CLICKHOUSE_USER')
db_password = os.getenv('CLICKHOUSE_PASSWORD')
table_name = 'st_timezone'
table_name_distr = 'st_timezone_distr'
port = 8123

client = clickhouse_connect.get_client(host=host, port=port, username=os.getenv(db_user), password=os.getenv(db_password))
database = 'vlad_meniailov'

logging.info("Создание БД если не существует на кластере")
client.command(f'''
    CREATE DATABASE IF NOT EXISTS {database} ON CLUSTER '{{cluster}}';
''')

# ⬇️ Cоздание таблиц(реплика и дистрибутивная)
logging.info("Удаление таблицы st_timezone_local на кластере")
client.command(f'''
    DROP TABLE IF EXISTS {database}.st_timezone_local ON CLUSTER 'company_cluster' SYNC;
''')

logging.info("Создание таблицы st_timezone_local на кластере")
client.command(f'''
    CREATE TABLE {database}.st_timezone_local ON CLUSTER 'company_cluster' (
        st_id Int,
        shop_name String,
        tz_code Int
    )
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{cluster}}/{{shard}}/vlad_meniailov/st_timezone_local', '{{replica}}')
    PARTITION BY (tz_code)
    ORDER BY (st_id);
''')

logging.info("Удаление таблицы st_timezone_distr на кластере")
client.command(f'''
    DROP TABLE IF EXISTS {database}.st_timezone_distr ON CLUSTER 'company_cluster';
''')

logging.info("Создание таблицы st_timezone_distr на кластере")
client.command(f'''
    CREATE TABLE {database}.st_timezone_distr ON CLUSTER 'company_cluster' AS {database}.st_timezone_local
    ENGINE = Distributed('company_cluster', vlad_meniailov, st_timezone_local, rand());
''')


logging.info("⬇️ Запись данных из DataFrame в ClickHouse")
# ⬇️ Запись данных из DataFrame в ClickHouse
result_df.write \
	.format("jdbc") \
	.option("url", jdbc_url) \
	.option("user", db_user) \
	.option("password", db_password) \
	.option("dbtable", table_name_distr) \
	.option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
	.mode("append") \
	.save()

logging.info("Таблица сохранена в Clickhouse!")
