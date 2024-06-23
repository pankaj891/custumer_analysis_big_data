from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StringType, DecimalType, StructField, StructType, DateType, DoubleType, IntegerType, Row
import json

mysql_info = StructType([
        StructField("DATE", StringType()),
        StructField("CITY_NAME", StringType()),
        StructField("WHEATHER_MAIN", StringType()),
        StructField("DESCRIPTION", StringType()),
        StructField("ICON", StringType()),
        StructField("TEMPERATURE", StringType()),
        StructField("FEELS_LIKE_TEMP", StringType()),
        StructField("HUMIDITY", StringType()),
        StructField("VISIBILITY", StringType()),
        StructField("WIND_SPEED", StringType()),
        StructField("CLOUD_ALL", StringType())
    ])

cql_info = StructType([
        StructField("longitude", StringType()),
        StructField("latitude", StringType()),
        StructField("wheather_main", StringType()),
        StructField("weather_description", StringType()),
        StructField("weather_icon", StringType()),
        StructField("base", StringType()),
        StructField("main_temp", StringType()),
        StructField("main_feels_like", StringType()),
        StructField("main_temp_min", StringType()),
        StructField("main_temp_max", StringType()),
        StructField("main_pressure", StringType()),
        StructField("main_humidity", StringType()),
        StructField("visibility", StringType()),
        StructField("wind_speed", StringType()),
        StructField("wind_deg", StringType()),
        StructField("clouds_all", StringType()),
        StructField("date", StringType()),
        StructField("country", StringType()),
        StructField("sunrise", StringType()),
        StructField("sunset", StringType()),
        StructField("timezone", StringType()),
        StructField("name", StringType()),
        StructField("cod", StringType())
    ])

# Create a SparkSession
spark = SparkSession.builder \
    .appName("StructuredStreamingKafkaWordCount") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

def put_data_in_mysql(batch_df,batchid):
    collected_data = batch_df.collect()
    #print("==> ",collected_data)
    if collected_data:
        list_of_rows = []
        for row in collected_data:
            value = row.value
            json_object = json.loads(value)
            my_rows = Row(json_object["dt"], json_object["name"], json_object["weather"][0]["main"],
                           json_object["weather"][0]["description"], json_object["weather"][0]["icon"],
                           json_object["main"]["temp"], json_object["main"]["feels_like"],
                           json_object["main"]["humidity"], json_object["visibility"], json_object["wind"]["speed"],
                           json_object["clouds"]["all"])
            list_of_rows.append(my_rows)
        #print("final put : ",list_of_rows)
        mydf = spark.createDataFrame(list_of_rows, mysql_info)
        #mydf.show()
        properties = {
            "user": "root",
            "driver": "com.mysql.cj.jdbc.Driver"  # JDBC driver for MySQL
        }
        mydf.write.jdbc(url='jdbc:mysql://localhost:3306/weatherforecast',table='weatherinfo',mode='append',properties=properties)

def put_data_in_cql(batch_df,batchid):
    collected_data = batch_df.collect()
    print("==> ", collected_data)
    if collected_data:
        list_of_rows = []
        for row in collected_data:
            value = row.value
            json_object = json.loads(value)
            my_rows = Row(str(json_object["coord"]["lon"]),str(json_object["coord"]["lat"]), str(json_object["weather"][0]["main"]),str(json_object["weather"][0]["description"]),
                          str(json_object["weather"][0]["icon"]),str(json_object["base"]),str(json_object["main"]["temp"]),str(json_object["main"]["feels_like"]),str(json_object["main"]["temp_min"]),str(json_object["main"]["temp_max"]),
                          str(json_object["main"]["pressure"]),str(json_object["main"]["humidity"]),str(json_object["visibility"]),str(json_object["wind"]["speed"]),str(json_object["wind"]["deg"]),
                          str(json_object["clouds"]["all"]),str(json_object["dt"]),str(json_object["sys"]["country"]),str(json_object["sys"]["sunrise"]),str(json_object["sys"]["sunset"]),
                          str(json_object["timezone"]),str(json_object["name"]),str(json_object["cod"]))
            list_of_rows.append(my_rows)
        print("final put : ", list_of_rows)
        mydf = spark.createDataFrame(list_of_rows, cql_info)
        mydf = mydf.withColumn("id",monotonically_increasing_id())
        print("in cql part")
        mydf.show()
        mydf.write.format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace","weatherforecast") \
        .option("table","weatherinfo") \
        .save()


# Subscribe to the Kafka topic
kafkaStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "custumer-analyze") \
    .option("startingOffsets","latest") \
    .load()

kafkaStream.printSchema()

kafka_source = kafkaStream.withColumn("value", kafkaStream["value"].cast("STRING")).select("value")

queryone = kafka_source.writeStream.outputMode("append").foreachBatch(put_data_in_mysql).trigger(processingTime="5 seconds") \
    .option("checkpointLocation","file:///Users/pankajpachori/PycharmProjects/SparkStreaming/check/").start()
querytwo = kafka_source.writeStream.outputMode("append").foreachBatch(put_data_in_cql).trigger(processingTime="5 seconds") \
           .option("checkpointLocation","file:///Users/pankajpachori/PycharmProjects/SparkStreaming/checkcql/").start()
# Wait for the query to terminate
queryone.awaitTermination()
querytwo.awaitTermination()



