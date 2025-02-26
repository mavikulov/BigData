from confluent_kafka import Consumer, Producer
from sklearn.preprocessing import LabelEncoder
import pandas as pd
import numpy as np
import json
import time 


DROPPED_COLUMNS = ['ship_mode', 'segment', 'country', 'city', 'state', 'region', 'category', 'sub-category', 'product_name',
                   'order_id', 'order_date', 'ship_date', 'customer_id', 'product_id', 'customer_name', 'unnamed:_0.1', 'unnamed:_0']


bootstrap_server_from = "localhost:9095,localhost:9097"
topic_from = "produce_row_data"
config_from = {
    "bootstrap.servers": bootstrap_server_from,
    "group.id": 'prep_consumers'
}

consumer = Consumer(config_from)
consumer.subscribe([topic_from])

bootstrap_server_to = "localhost:9095,localhost:9097"
topic_to = "produce_preprocessed_data"
config_to = {
    "bootstrap.servers": bootstrap_server_to
}

producer = Producer(config_to)


def preprocess_record_series(series):
    series.index = series.index.str.lower().str.replace(" ", "_")
    series = series.drop(labels=DROPPED_COLUMNS, errors='ignore')
    return series


def preprocessing_data():
    while True:
        message = consumer.poll(1)
        if message is not None:
            data_from_raw_data_producer = json.loads(message.value().decode("utf-8")) 
            preprocessed_data = preprocess_record_series(pd.Series(json.loads(data_from_raw_data_producer)))
            producer.produce(topic_to, key="1", value=json.dumps(preprocessed_data.to_dict()))
            producer.flush()


if __name__ == "__main__":
    preprocessing_data()