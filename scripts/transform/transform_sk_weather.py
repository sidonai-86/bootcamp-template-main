import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("WeatherS3toClickHouse") \
        .getOrCreate()

    # --- ЛОГИКА ДЛЯ МАНИФЕСТА ---
    # Airflow передаст список новых файлов через запятую
    s3_files = os.getenv("S3_PATH_DATA")

    if s3_files:
        print(f"DEBUG: Processing specific files from manifest: {s3_files}")
        # Превращаем строку в список путей
        input_paths = s3_files.split(",")
        # Читаем только новые файлы
        df = spark.read.parquet(*input_paths)
    else:
        print("DEBUG: No manifest found. Reading entire directory.")
        # Фолбэк: если запускаем руками или переменная пуста
        input_path = "s3a://dev/sk8california/weather/daily/"
        df = spark.read.option("recursiveFileLookup", "true").parquet(input_path)
    # ----------------------------

    print(f"DEBUG: Rows found to process: {df.count()}")

    # 2. Преобразование типов
    df_transformed = df.withColumn("time", col("time").cast("timestamp")) \
                       .withColumn("update_at", col("update_at").cast("timestamp"))

    # 3. Запись в ClickHouse
    df_transformed.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse01:8123/sk8california") \
        .option("dbtable", "weather_data") \
        .option("user", "default") \
        .option("password", "") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("batchsize", "10000") \
        .option("socket_timeout", "300000") \
        .mode("append") \
        .save()

    print("Successfully written to ClickHouse")
    spark.stop()

if __name__ == "__main__":
    main()