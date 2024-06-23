import requests
from confluent_kafka import Producer

# Define Kafka broker(s) and topic
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker(s)
topic = 'custumer-analyze'  # Replace with the name of the Kafka topic you want to produce to
url = 'https://api.openweathermap.org/data/2.5/weather?q=indore&appid=0c305b33e6666d3ba4fde132b9efa498'

# Create a Kafka producer configuration
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'python-producer'  # A unique identifier for the producer
}

# Create a Kafka producer instance
producer = Producer(producer_config)

count = 0
while count < 3:
    data = requests.get(url)
    produce_data = str(data.json())
    final = produce_data.replace("'","\"")
    print(final)
    ack = producer.produce(topic, value=final)
    print("ack : ", ack)
    count = count + 1

producer.flush()