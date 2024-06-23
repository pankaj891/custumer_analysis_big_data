from confluent_kafka import Producer
import csv
import json

# Define Kafka broker(s) and topic
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker(s)
topic = 'custumer-analyze'  # Replace with the name of the Kafka topic you want to produce to

# Create a Kafka producer configuration
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'python-producer'  # A unique identifier for the producer
}

# Create a Kafka producer instance
producer = Producer(producer_config)

with open("/Users/pankajpachori/PycharmProjects/Ecommerce_Customers.csv","r") as csv_file:
    csv_reader = csv.DictReader(csv_file)
    csv_data = [row for row in csv_reader]

json_data = json.dumps(csv_data, indent=4)
data_list = json.loads(json_data)

for data in data_list:
    print(data)
    ack = producer.produce(topic, value=str(data))
    print("ack : ",ack)

producer.flush()
