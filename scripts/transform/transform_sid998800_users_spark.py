import os
import requests
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import col, regexp_replace
from clickhouse_manager import ClickHouseManager

# ⬇️ Параметры подключения к CLICKHOUSE
jdbc_url = os.getenv("CLICKHOUSE_JDBC_URL")
db_host = os.getenv("CLICKHOUSE_HOST")
db_user = os.getenv("CLICKHOUSE_USER")
db_password = os.getenv("CLICKHOUSE_PASSWORD")
table_name = f"{os.getenv('TABLE_NAME')}_local"
distributed_table_name = os.getenv("TABLE_NAME")
cluster_name = os.getenv("CLUSTER_NAME")
ch_database = os.getenv("DATABASE")

# Инициализация Spark tg_users
spark = (
    SparkSession.builder.appName("S3toClickNews")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)

ch_database = spark.conf.get("spark.DATABASE")
table_name = spark.conf.get("spark.TABLE_NAME")
distributed_table_name = f"{spark.conf.get('spark.TABLE_NAME')}_local"
cluster_name = spark.conf.get("spark.CLUSTER_NAME")

# Инициализация ClickHouseManager
ch_manager = ClickHouseManager(
    host=db_host, user=db_user, password=db_password, database=ch_database
)
print(f"table_name ===== {table_name}")

# Ссылки на датасеты
urls = [
    "https://huggingface.co/datasets/halltape/users/resolve/main/google_users.csv",
    "https://huggingface.co/datasets/halltape/users/resolve/main/other_users.csv",
    "https://huggingface.co/datasets/halltape/users/resolve/main/yandex_users.csv",
]

# Создаем список из временных файлов
temp_files = []
for url in urls:
    try:
        print(f"Скачиваю {url}")
        response = requests.get(url)
        response.raise_for_status()

        # Создаем временный файл
        temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv")
        temp_file.write(response.text)
        temp_file.close()

        temp_files.append(temp_file.name)
        print(f"Сохранен как {temp_file.name}")

    except Exception as e:
        print(f"Ошибка при скачивании {url}: {e}")

# Читай в датафрейм наши датасеты
df = spark.read.csv(temp_files, header=True)

print(f"Общее количество строк: {df.count()}")
print(f"Колонки: {df.columns}")


# Переименовываем и удаляем ненужные колонки согласно ТЗ
def transform_users(df):
    drop_cols = ("pk_id", "update_at")
    df_all = (
        df.withColumn("registration_date", col("update_at").cast("date"))
        .withColumn("tg_id", regexp_replace("tg_id", "@", "").cast(LongType()))
        .withColumn("tg_nickname", regexp_replace("tg_nickname", "@", ""))
    )
    result = (
        df_all.distinct()
        .withColumnRenamed("tg_id", "telegram_id")
        .withColumnRenamed("tg_nickname", "user_nickname")
        .drop(*drop_cols)
    )
    return result


result_df = transform_users(df)

drop_local = f"""
    DROP TABLE IF EXISTS {ch_database}.{table_name} ON CLUSTER {cluster_name} SYNC
"""

create_local = f"""
    CREATE TABLE IF NOT EXISTS {ch_database}.{table_name} ON CLUSTER {cluster_name} (
        telegram_id UInt64,
        user_nickname String,
        registration_date Date
    )
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{cluster}}/{{shard}}/{ch_database}_{table_name}', '{{replica}}')
    PARTITION BY toYYYYMM(registration_date)
    ORDER BY (telegram_id)
    """

create_distributed = f"""
    CREATE TABLE IF NOT EXISTS {ch_database}.{distributed_table_name} ON CLUSTER {cluster_name}
    AS {ch_database}.{table_name}
    ENGINE = Distributed('{cluster_name}', '{ch_database}', {table_name}, halfMD5(telegram_id));
    """

# Создаем таблицы через кликхаус менеджер
ch_manager.execute_sql(drop_local)
ch_manager.execute_sql(create_local)
ch_manager.execute_sql(create_distributed)
print("Таблицы успешно созданы!")

# Запись в ClickHouse
(
    result_df.write.format("jdbc")
    .option("url", jdbc_url)
    .option("user", db_user)
    .option("password", db_password)
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .option("dbtable", distributed_table_name)
    .mode("append")
    .save()
)

df_count = result_df.count()
print(f"LOADED_ROWS_COUNT:{df_count} строк")
print("✅ Батч успешно загружен в ClickHouse.")

# Записываем число строк в файл для дальнейшего чтения в Airflow
os.makedirs("/opt/airflow/data", exist_ok=True)
with open("/opt/airflow/data/spark_count.txt", "w") as f:
    f.write(str(df_count))

# Удаляем временные файлы
for file in temp_files:
    os.remove(file)
print("✅ Временные файлы удалены")
