import requests
import csv
import io
from dotenv import load_dotenv
import os
from DataStrategy import *


def download_csv_data(url):
    response = requests.get(url)
    content = response.content
    return content


def read_csv_data(content, limit=None):
    file_object = io.StringIO(content.decode('utf-8'))
    reader = csv.DictReader(file_object)
    data = []
    if limit is not None:
        for row in reader:
            row = {key.lower(): value for key, value in row.items()}
            data.append(row)
            if len(data) >= limit:
                break
    else:
        data = list(reader)
    return data


def main():
    load_dotenv()
    strategy = os.getenv('STRATEGY')
    url = os.getenv('URL')
    limit = int(os.getenv('LIMIT'))
    host = os.getenv('HOST')

    content = download_csv_data(url)

    data = read_csv_data(content, limit)

    processor = None
    if strategy == "console":
        processor = DataProcessor(ConsoleDataProcessingStrategy())
    elif strategy == "kafka":
        processor = DataProcessor(KafkaDataProcessingStrategy(host))
    else:
        print("There is no such strategy.")

    processor.process(data)


if __name__ == '__main__':
    main()
