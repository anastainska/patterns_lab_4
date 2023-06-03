from abc import ABC, abstractmethod
from time import sleep
from json import dumps
from kafka.producer import KafkaProducer


class DataProcessingStrategy(ABC):
    @abstractmethod
    def process_data(self, data):
        pass


class ConsoleDataProcessingStrategy(DataProcessingStrategy):
    def process_data(self, data):
        print("Console data:", data)


class KafkaDataProcessingStrategy(DataProcessingStrategy):
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            api_version=(0, 11, 5),
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )

    def process_data(self, data):
        self.producer.send('data_csv', value=data)
        sleep(0.3)


class DataProcessor:
    def __init__(self, strategy):
        self.strategy = strategy

    def process(self, data_list):
        for data in data_list:
            self.strategy.process_data(data)
