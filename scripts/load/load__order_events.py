import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.functions import from_unixtime, col, to_date, from_json


kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP')
kafka_topic = os.getenv('KAFKA_TOPIC')
s3_path = os.getenv('S3_PATH')


spark = SparkSession.builder \
    .appName("KafkaToS3OrderEvents") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

# Чтение из Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()


# Описание схемы JSON сообщения
schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("ts", LongType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("ts", LongType(), True)
    ]), True),
    StructField("source", StructType([]), True),  # если не используешь, можно пустым
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

print(schema)

json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .where(''' data.after IS NOT NULL ''') \
    .select("data.after.*")


# Преобразуем ts (Unix → UTC Timestamp → Date)
processed_df = json_df \
    .withColumn("ts_sec", (col("ts") / 1_000_000).cast("double")) \
    .withColumn("ts_utc", from_unixtime(col("ts_sec")).cast("timestamp")) \
    .withColumn("event_date", to_date(col("ts_utc"))) \
    .drop("ts", "ts_sec")


# Запись в S3 с партиционированием
processed_df.writeStream \
    .format("parquet") \
    .queryName("order_events") \
    .option("path", s3_path) \
    .option("checkpointLocation", s3_path + "/_checkpoint/") \
    .partitionBy("event_date") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
