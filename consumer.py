from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType

spark = SparkSession.builder \
    .appName("Kafka Sensor Consumer") \
    .master("local[*]") \
    .getOrCreate()

# konfigurasi Kafka
kafka_topic = "sensor-suhu"
kafka_bootstrap_servers = "localhost:9092"

# skema data
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("sensor_id", StringType()) \
    .add("temperature", FloatType())

# stream data dari Kafka
sensor_data_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# value dari Kafka ke tipe string dan mem-parsing JSON
sensor_data_df = sensor_data_df.selectExpr("CAST(value AS STRING)")
sensor_data_df = sensor_data_df \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# data suhu di atas 80°C
filtered_df = sensor_data_df.filter(col("temperature") > 80)

# data yang suhu-nya melebihi 80°C ke display console
query = filtered_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
