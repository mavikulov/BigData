from confluent_kafka import Producer
import pandas as pd
import json
import random
import time
import os


bootstrap_server = "localhost:9095,localhost:9097" 
topic = "produce_row_data" 
config = {
    "bootstrap.servers": bootstrap_server
}

producer = Producer(config)


def produce_raw_data(data):
    random_indices = random.sample(range(len(data)), len(data))
    for index in random_indices:
        random_row = data.iloc[index].to_json()
        producer.produce(topic, key="1", value=json.dumps(random_row))
        producer.flush()
        time.sleep(0.2)


if __name__ == "__main__":
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, 'data', 'train.csv')
    data = pd.read_csv(file_path, encoding='utf-8')
    produce_raw_data(data)
    