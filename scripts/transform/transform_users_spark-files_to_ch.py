# Мне необходимо скачать 3 файла по ссылке спарком.
# Далее мне необходимо эти данные преобразовать в нужный формат по ТЗ.
# И после этого загрузить данные в CH.
# Ну и после того как я убедился что у меня всё работает сделать из этого ДАГ.

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, regexp_replace
from pyspark.sql.types import LongType
from pyspark import SparkFiles
import clickhouse_connect
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ⬇️ Параметры подключения к CLICKHOUSE

jdbc_url = 'jdbc:clickhouse://clickhouse01:8123/vlad_meniailov'
host = 'clickhouse01'
db_user = os.getenv('CLICKHOUSE_USER')
db_password = os.getenv('CLICKHOUSE_PASSWORD')
table_name_local = 'users_local'
table_name_distr = 'users'
port = 8123

url_google_users = "https://huggingface.co/datasets/halltape/users/resolve/main/google_users.csv"
url_other_users = "https://huggingface.co/datasets/halltape/users/resolve/main/other_users.csv"
url_yandex_users = "https://huggingface.co/datasets/halltape/users/resolve/main/yandex_users.csv"


spark = (SparkSession.builder
    .appName("SparkExample")
    .config("spark.jars.packages", 
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
        "org.postgresql:postgresql:42.5.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        )
    .config("spark.files.overwrite", "true")
    .getOrCreate())

def add_files_to_spark_files(url_first, url_second, url_third):
    logging.info(f"Try add files to SparkFiles from urls")
    
    spark.sparkContext.addFile(url_first)
    logging.info(f"Added file to SparkFiles from url:{url_first}")

    spark.sparkContext.addFile(url_second)
    logging.info(f"Added file to SparkFiles from url:{url_second}")

    spark.sparkContext.addFile(url_third)
    logging.info(f"Added file to SparkFiles from url:{url_third}")

def transform_users(df_google_users, df_other_users, df_yandex_users):
    drop_cols = ('pk_id', 'update_at')

    df_all_users = df_google_users.unionAll(df_other_users).unionAll(df_yandex_users) \
        .withColumn("registration_date", col("update_at").cast("date")) \
        .withColumn('tg_id', regexp_replace("tg_id", "@", "").cast(LongType())) \
        .withColumn('tg_nickname', regexp_replace("tg_nickname", "@", ""))

    result_df = df_all_users.distinct() \
        .withColumnRenamed('tg_id', "telegram_id") \
        .withColumnRenamed('tg_nickname', "user_nickname") \
        .drop(*drop_cols)

    return result_df


def ch_prepare_action():
    client = clickhouse_connect.get_client(host=host, port=port, username=os.getenv(db_user), password=os.getenv(db_password))
    database = 'vlad_meniailov'
    
    logging.info("Создание БД, если такой не существует в CH на кластере")
    client.command(f'''
        CREATE DATABASE IF NOT EXISTS {database} ON CLUSTER '{{cluster}}';
    ''')
    
    # ⬇️ Cоздание таблиц(реплика и дистрибутивная)
    logging.info(f"Удаление таблицы {table_name_local} на кластере")
    client.command(f'''
        DROP TABLE IF EXISTS {database}.{table_name_local} ON CLUSTER 'company_cluster' SYNC;
    ''')
    
    logging.info(f"Создание таблицы {table_name_local} на кластере")
    client.command(f'''
        CREATE TABLE {database}.{table_name_local} ON CLUSTER 'company_cluster' (
            telegram_id Int64,
            user_nickname String,
            registration_date Date
        )
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{cluster}}/{{shard}}/vlad_meniailov/users_local', '{{replica}}')
        PARTITION BY (registration_date)
        ORDER BY (telegram_id);
    ''')
    
    logging.info(f"Удаление таблицы {table_name_distr} на кластере")
    client.command(f'''
        DROP TABLE IF EXISTS {database}.{table_name_distr} ON CLUSTER 'company_cluster';
    ''')
    
    logging.info(f"Создание таблицы {table_name_distr} на кластере")
    client.command(f'''
        CREATE TABLE {database}.{table_name_distr} ON CLUSTER 'company_cluster' AS {database}.users_local
        ENGINE = Distributed('company_cluster', vlad_meniailov, users_local, telegram_id);
    ''')

def write_df_to_ch(result_df):
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

    logging.info(f"Данные сохранены в таблицу {table_name_distr} в Clickhouse!")

try:
    add_files_to_spark_files(url_google_users, url_other_users, url_yandex_users)

    df_google_users = spark.read.csv(SparkFiles.get("google_users.csv"), header=True)
    df_other_users = spark.read.csv(SparkFiles.get("other_users.csv"), header=True)
    df_yandex_users = spark.read.csv(SparkFiles.get("yandex_users.csv"), header=True)

    result = transform_users(df_google_users, df_other_users, df_yandex_users)

    ch_prepare_action()
    write_df_to_ch(result)
except Exception as e:
    logging.error(f"Error in main action: {e}")
finally:
    spark.stop()