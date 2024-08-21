from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType, TimestampType
from pyspark.sql.functions import split,from_json,col
import pyspark.sql.functions as F
from pyspark.sql.functions import *
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0

odometrySchema = StructType([
                StructField("id",IntegerType(),False),
                StructField("eventTimestamp", TimestampType(), False),
                StructField("temperature",FloatType(),False),
                StructField("pressure",FloatType(),False),
            ])

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .config("spark.driver.host", "localhost")\
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")



sensorStreamDF = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
  .option("subscribe", "rosmsgs") \
  .option("delimeter",",") \
  .option("startingOffsets", "earliest") \
  .load() 


sensorStreamDF1 = sensorStreamDF.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),odometrySchema).alias("data")).select("data.*")

# query = sensorStreamDF.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()



average_temperature_and_pressure_df = sensorStreamDF1 \
    .withWatermark("eventTimestamp", "10 minutes") \
    .groupBy(window("eventTimestamp", "10 minutes", "5 minutes"), "temperature", "pressure") \
    .agg(avg("temperature").alias("average_temperature"), avg("pressure").alias("average_pressure")) \
    .select(col("window.start").alias("start"), col("window.end").alias("end"), col("average_temperature"), col("average_pressure")) \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()

# average_temperature_and_pressure_df = sensorStreamDF \
# #     .withWatermark("eventTimestamp", "3 minutes") \
# #     .groupBy(window("eventTimestamp", "3 minutes"), "temperature", "pressure") \
# #     .agg(avg("temperature").alias("average_temperature"), avg("pressure").alias("average_pressure")) \
# #     .select(col("window.start").alias("start"), col("window.end").alias("end"), col("average_temperature"), col("average_pressure")) \
# #     .writeStream \
# #     .outputMode("update") \
# #     .format("console") \
# #     .option("truncate", False) \
# #     .start() \
# #     .awaitTermination()