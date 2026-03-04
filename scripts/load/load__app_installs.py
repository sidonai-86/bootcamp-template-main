from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import os


jdbc_url = os.getenv('POSTGRES_JDBC_URL')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
table_name = os.getenv('TABLE_NAME')
s3_path = os.getenv('S3_APP_INSTALLS_PATH')


# Инициализация Spark
spark = SparkSession.builder \
    .appName("JdbcToS3AppInstalls") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()


# Проверка, есть ли уже файлы в S3
try:
    existing_data_df = spark.read.parquet(s3_path)
    max_ts = existing_data_df.selectExpr("MAX(ts) as max_ts").collect()[0]["max_ts"]
    print(f"🔁 Инкрементальная загрузка с ts > {max_ts}")
    predicate = f"ts > timestamp '{max_ts}'"
except Exception as e:
    print("🆕 Данных в S3 нет, загрузим всё из базы.")
    predicate = "1=1"

# Чтение из PostgreSQL
jdbc_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("dbtable", table_name) \
    .option("fetchsize", 1000) \
    .option("driver", "org.postgresql.Driver") \
    .option("pushDownPredicate", "true") \
    .load() \
    .filter(predicate)

# Обогащение датой для партиционирования
df_with_partition = jdbc_df \
    .withColumn("event_date", to_date(col("ts")))


# Запись в S3 с партиционированием
df_with_partition.write \
    .mode("append") \
    .partitionBy("event_date") \
    .parquet(s3_path)

print("✅ Загрузка завершена.")
