import boto3
import csv
import json

sqs_client = boto3.client("sqs",region_name="ap-south-1",aws_access_key_id="AKIAVW3XBM6Z6DSEOLO4",aws_secret_access_key="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")

queue = sqs_client.get_queue_url(QueueName= "custumer-data.fifo")
message_group_id = "custumer-analysis"

with open("/Users/pankajpachori/Downloads/Ecommerce_Customers.csv","r") as csv_file:
    csv_reader = csv.DictReader(csv_file)
    csv_data = [row for row in csv_reader]

json_data = json.dumps(csv_data, indent=4)
data_list = json.loads(json_data)
for data in data_list:
    print(data)
    ack = sqs_client.send_message(QueueUrl=queue['QueueUrl'], MessageBody= str(data),MessageGroupId = message_group_id)
    print("ack : ",ack)
