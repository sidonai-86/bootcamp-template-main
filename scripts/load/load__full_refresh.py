from pyspark.sql import SparkSession
import os

jdbc_url = os.getenv('POSTGRES_JDBC_URL')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
table_name = os.getenv('TABLE_NAME')
s3_path = os.getenv('S3_PATH')

# Инициализация Spark
spark = SparkSession.builder \
    .appName("JdbcToS3Regions") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()


# Чтение из PostgreSQL
jdbc_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("dbtable", table_name) \
    .option("fetchsize", 1000) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Запись в S3 с партиционированием
jdbc_df.write \
    .mode("overwrite") \
    .parquet(s3_path)

print("✅ Загрузка завершена.")
