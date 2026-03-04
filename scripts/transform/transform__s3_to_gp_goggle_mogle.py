import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime

GP_HOST = os.environ.get("GP_HOST")
GP_PORT = os.environ.get("GP_PORT")
GP_DB = os.environ.get("GP_DB")
GP_USER = os.environ.get("GP_USER")
GP_PASSWORD = os.environ.get("GP_PASSWORD")

JDBC_URL = f"jdbc:postgresql://{GP_HOST}:{GP_PORT}/{GP_DB}"
GP_TABLE = "goggle_mogle.btc_trades_raw"

S3_BUCKET_PATH = "s3a://dev/goggle_mogle/btc_trades"
S3_ARCHIVE_PATH = "s3a://dev/goggle_mogle/archive/btc_trades"

# 1. Инициализация Spark
spark = SparkSession.builder.appName("S3_to_Greenplum").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("📥 Читаем исторические данные из S3 (MinIO)...")

# 2. Чтение файлов
try:
    df = spark.read.parquet(f"{S3_BUCKET_PATH}/*.parquet")
except Exception as e:
    print("🤷‍♂️ Нет новых файлов для загрузки.")
    sys.exit(0)

# 3. Трансформация данных (BIGINT -> TIMESTAMP)
df_transformed = df \
    .withColumn("trade_time", from_unixtime(col("trade_time") / 1000).cast("timestamp")) \
    .withColumn("event_time", from_unixtime(col("event_time") / 1000).cast("timestamp"))

# 4. Запись в Greenplum
print(f"📤 Записываем данные в Greenplum (таблица {GP_TABLE})...")
df_transformed.write \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", GP_TABLE) \
    .option("user", GP_USER) \
    .option("password", GP_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("✅ Данные успешно загружены в Greenplum!")

# 5. ПЕРЕНОС ФАЙЛОВ В АРХИВ
print("📦 Начинаем перенос обработанных файлов в архив...")

# Получаем доступ к файловой системе Hadoop, которая работает с S3
sc = spark.sparkContext
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
hadoop_conf = sc._jsc.hadoopConfiguration()

fs = FileSystem.get(URI("s3a://dev/"), hadoop_conf)

source_dir = Path(S3_BUCKET_PATH)
archive_dir = Path(S3_ARCHIVE_PATH)

# Создаем папку архива, если ее еще нет
if not fs.exists(archive_dir):
    fs.mkdirs(archive_dir)

# Получаем список файлов и переносим их по одному
files = fs.listStatus(source_dir)
moved_count = 0

for file_status in files:
    file_path = file_status.getPath()
    if file_path.getName().endswith(".parquet"):
        dest_path = Path(f"{S3_ARCHIVE_PATH}/{file_path.getName()}")
        # rename в S3 = копирование + удаление оригинала
        fs.rename(file_path, dest_path)
        moved_count += 1

print(f"🧹 Успешно перемещено файлов в архив: {moved_count}")