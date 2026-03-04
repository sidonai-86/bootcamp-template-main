from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

spark = SparkSession.builder \
    .appName("kafka_to_s3_batch") \
    .getOrCreate()

spark.conf.set("spark.sql.caseSensitive", "true")
spark.sparkContext.setLogLevel("WARN")

S3_BUCKET_PATH = "s3a://dev/goggle_mogle/btc_trades/"
CHECKPOINT_PATH = "s3a://dev/goggle_mogle/checkpoints/btc_trades_raw/"

schema = StructType([
    StructField("t", LongType()),    # trade_id
    StructField("s", StringType()),  # symbol
    StructField("p", StringType()),  # price
    StructField("q", StringType()),  # quantity
    StructField("T", LongType()),    # trade_time
    StructField("E", LongType()),    # event_time
    StructField("m", BooleanType())  # is_buyer_maker
])

print("🚀 Запуск микро-батча: Чтение из Kafka...")

# Читаем из Kafka с защитой от OOM
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "goggle_mogle") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 50000) \
    .load()


# 5. Парсим JSON и СРАЗУ переименовываем колонки при извлечении
final_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(
        get_json_object(col("json_str"), "$.trade_id").cast("long").alias("trade_id"),
        get_json_object(col("json_str"), "$.symbol").cast("string").alias("symbol"),
        get_json_object(col("json_str"), "$.price").cast("double").alias("price"),
        get_json_object(col("json_str"), "$.quantity").cast("double").alias("quantity"),
        get_json_object(col("json_str"), "$.amount_usdt").cast("double").alias("amount_usdt"),
        get_json_object(col("json_str"), "$.trade_time").cast("long").alias("trade_time"),
        get_json_object(col("json_str"), "$.event_time").cast("long").alias("event_time"),
        get_json_object(col("json_str"), "$.is_buyer_maker").cast("boolean").alias("is_buyer_maker")
    )

# Запись в S3
print("💾 Запись данных в MinIO...")

query = final_df.writeStream \
    .format("parquet") \
    .option("path", S3_BUCKET_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

print("✅ Микро-батч успешно завершен. Данные сохранены в S3 в формате Parquet.")