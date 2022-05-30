from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the lines into words
words = lines.select((split(lines.value,",")[0]).alias("Sensores"),(split(lines.value,",")[1].alias("Temperatura").cast("int")))

# Generate running word count
wordCounts = words.groupBy("Sensores").avg("Temperatura")

query = wordCounts \
    .writeStream \
    .outputMode("Complete") \
    .format("console") \
    .start()

query.awaitTermination()
