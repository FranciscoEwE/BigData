from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,avg
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,conv,hex
import boto3
s3 = boto3.client('s3')

spark = SparkSession \
    .builder \
    .appName("Valores") \
    .getOrCreate()
print("Works")
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","172.31.29.144:9092") \
    .option("subscribe","quickstart-events") \
    .load()
print("Finish")


df=lines.withColumn("Price", conv(col("value"), 16, 16).cast("bigint"))
df2=df.withColumn("PRICE1", df["PRICE"]).withColumn("PRICE2",df["PRICE"])
query=df2.agg({'PRICE': 'avg','PRICE1': 'max','PRICE2': 'min'})
query=query.withColumnRenamed('min(PRICE2)', 'Min Value')
query=query.withColumnRenamed('max(PRICE1)', 'Max Value')
query=query.withColumnRenamed('avg(PRICE)', 'Avg Value')


writer = query \
    .writeStream \
    .outputMode("Complete") \
    .format("console") \
    .start().awaitTermination()
