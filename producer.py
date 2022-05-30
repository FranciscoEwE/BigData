
from kafka import KafkaProducer
from pyspark.sql import SQLContext , SparkSession
from pyspark.sql import functions as f
import time

spark = SparkSession.builder.master("local[*]").getOrCreate()
producer = KafkaProducer(bootstrap_servers=['172.31.29.144:9092'])

sc = spark.sparkContext
sqlContext = SQLContext(sc)
df = sqlContext.read.csv("SPY_TICK_TRADE.csv",header = "true")

sample_d = df.rdd.map(lambda x: (x.PRICE))

for r in sample_d.collect():
        time.sleep(2)
        st = str(r).encode()
        producer.send('quickstart-events',st )
producer.flush()
