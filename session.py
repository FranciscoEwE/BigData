from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,avg
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,conv,hex
import argparse

spark = SparkSession \
    .builder \
    .appName("Sensor") \
    .getOrCreate()

print("Works")

parser = argparse.ArgumentParser()
parser.add_argument("--maximo", help="Maximo valor permitido :")
args = parser.parse_args()
if args.maximo:
    maximo = args.maximo

lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
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
    .outputMode("complete") \
    .format("console") \
    .start().awaitTermination()
df2.write.option("header","true").csv("s3://parcial3bigdata/DatosAvg/")
