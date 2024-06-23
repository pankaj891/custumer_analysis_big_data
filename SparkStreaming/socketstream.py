from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("StructuredStreamingKafkaWordCount") \
    .getOrCreate()

# Subscribe to the Kafka topic
kafkaStream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "9091") \
    .load()

print(kafkaStream.isStreaming)
kafkaStream.printSchema()
write_query = kafkaStream.writeStream.format("console").start()
write_query.awaitTermination()