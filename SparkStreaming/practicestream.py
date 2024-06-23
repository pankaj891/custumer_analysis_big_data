from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("StructuredStreamingKafkaWordCount") \
    .getOrCreate()

# Subscribe to the Kafka topic
kafkaStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "custumer-analyze") \
    .option("startingOffsets","latest") \
    .load()

kafkaStream.printSchema()

kafka_source = kafkaStream.select("offset")

query = kafka_source.writeStream.outputMode("append").format("console").trigger(processingTime="5 second") \
    .option("checkpointLocation","file:///Users/pankajpachori/PycharmProjects/SparkStreaming/checkcql/").start()
query.awaitTermination()



