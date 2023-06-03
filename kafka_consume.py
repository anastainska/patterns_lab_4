import os

from dotenv import load_dotenv
from kafka import KafkaConsumer
from json import loads

load_dotenv()
host = os.getenv('HOST')

my_consumer = KafkaConsumer(
    'data_csv',
    bootstrap_servers=host,
    api_version=(0, 11, 5),
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

for message in my_consumer:
    data = message.value
    print("Kafka:", data)
