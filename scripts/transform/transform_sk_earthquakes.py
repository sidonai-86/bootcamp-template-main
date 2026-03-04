import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, when, size, date_trunc

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("S3_to_CH_Final_Production_Ready") \
        .getOrCreate()

    # 1. Получаем переменные окружения
    data_paths_raw = os.getenv("S3_PATH_DATA")
    if not data_paths_raw:
        logger.error("❌ No paths provided in S3_PATH_DATA")
        spark.stop()
        exit(1)

    data_paths = [p.strip() for p in data_paths_raw.split(",") if p.strip()]
    
    ch_url = os.getenv("CLICKHOUSE_JDBC_URL")
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_pass = os.getenv("CLICKHOUSE_PASSWORD", "")
    table_name = os.getenv("TABLE_NAME", "sk8california.earthquakes")

    try:
        logger.info(f"🚀 Reading Parquet files: {data_paths}")
        df = spark.read.parquet(*data_paths)
        
        # Разделение колонки Place на "Место" и "Регион"
        place_split = split(col("properties_place"), ", ")

        # 2. ТРАНСФОРМАЦИЯ С ОЧИСТКОЙ ВРЕМЕНИ И РАЗДЕЛЕНИЕМ РЕГИОНОВ
        transformed_df = df.select(
            col("id").cast("string").alias("event_code"),
            col("properties_mag").cast("float").alias("magnitude"), 
            
            # Чистим место и регион
            trim(place_split.getItem(0)).alias("place"),
            when(size(place_split) > 1, trim(place_split.getItem(1)))
            .otherwise("Other")
            .alias("region"),

            # --- РАБОТА С ДАТАМИ ---
            # Обрезаем до секунд: будет 2026-02-08 14:21:48
            date_trunc("second", col("properties_time").cast("timestamp")).alias("event_time"),
            
            # Обрезаем до минут: будет 2026-02-08 14:21:00
            date_trunc("minute", col("update_at").cast("timestamp")).alias("update_at"),
            # -----------------------

            col("properties_tsunami").cast("int").alias("tsunami_warning"),
            col("geometry_coordinates").getItem(0).cast("double").alias("longitude"),
            col("geometry_coordinates").getItem(1).cast("double").alias("latitude"),
            col("geometry_coordinates").getItem(2).cast("double").alias("depth")
        )

        logger.info(f"📥 Writing to ClickHouse table: {table_name}")

        # 3. Запись в ClickHouse
        transformed_df.write \
            .format("jdbc") \
            .option("url", ch_url) \
            .option("dbtable", table_name) \
            .option("user", ch_user) \
            .option("password", ch_pass) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()
            
        logger.info("✅ Data successfully loaded! Everything looks clean.")
        
    except Exception as e:
        logger.error(f"❌ Spark Error: {str(e)}")
        raise
    finally:
        spark.stop()