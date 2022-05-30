from kafka import KafkaProducer
from pyspark.sql import SQLContext , SparkSession
from pyspark.sql import functions as f
import time
import boto3
import pandas as pd

spark = SparkSession.builder.master("local[*]").getOrCreate()
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

sc = spark.sparkContext
sqlContext = SQLContext(sc)
df = sqlContext.read.csv("SPY_TICK_TRADE.csv",header = "true")

sample_d = df.rdd.map(lambda x: (x.PRICE))

for r in sample_d.collect():
        time.sleep(2)
        st = str(r).encode()
        producer.send('quickstart-events',st )
producer.flush()
