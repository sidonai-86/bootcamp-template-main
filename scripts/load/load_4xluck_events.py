import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType
)
import pyspark.sql.functions as f


kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP")
kafka_topic = os.getenv("KAFKA_TOPIC")
s3_path = os.getenv("S3_PATH")

spark = (
    SparkSession.builder.appName("KafkaToS3OrderEvents")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)

# Чтение из Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# Описание схемы JSON сообщения
schema = StructType(
    [
        StructField(
            "before",
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("order_id", IntegerType(), True),
                    StructField("status", StringType(), True),
                    StructField("ts", LongType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "after",
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("order_id", IntegerType(), True),
                    StructField("status", StringType(), True),
                    StructField("ts", LongType(), True),
                ]
            ),
            True,
        ),
    ]
)

result_df = (
    df.select(f.col("key"), f.col("topic"), f.col("value").cast("string"))
    .withColumn("data", f.from_json(f.col("value"), schema))
    .drop(f.col("value"))
    .filter(f.col("data.after").isNotNull())
    .select("topic", "data.after.*")
    .withColumn("ts", (f.to_timestamp(f.col("ts") / 1_000_000)))
    .withColumn("event_date", f.to_date(f.col("ts")))
)

result_df.writeStream \
    .format("parquet") \
    .queryName("order_events") \
    .option("path", s3_path) \
    .option("checkpointLocation", s3_path + "/_checkpoint/") \
    .partitionBy("event_date") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start() \
    .awaitTermination()
