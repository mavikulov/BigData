from confluent_kafka import Producer, Consumer
from catboost import CatBoostRegressor
from river.metrics import MAE, MSE
import pandas as pd
import json
import time


TARGET_COLUMN = "sales"


bootstrap_server_from = "localhost:9095,localhost:9097"
topic_from = "produce_preprocessed_data"
config_from = {
    "bootstrap.servers": bootstrap_server_from,
    "group.id": 'ML_consumers'
}

consumer = Consumer(config_from)
consumer.subscribe([topic_from])

bootstrap_server_to = "localhost:9095,localhost:9097"
topic_to = "ml_topic"
config_to = {
    "bootstrap.servers": bootstrap_server_to
}

producer = Producer(config_to)

def online_learning_loop():
    catboost_regressor = CatBoostRegressor()
    catboost_regressor.load_model('./Notebooks/catboostregressor.model')
    mse = MSE()
    mae = MAE()
    while True:
        message = consumer.poll(1)
        if message is not None:
            data_from_preprocess_producer = json.loads(message.value().decode("utf-8"))
            data = pd.Series(data_from_preprocess_producer)
            target = data[TARGET_COLUMN]
            X = data.drop(labels=TARGET_COLUMN)
            pred = catboost_regressor.predict(X)
            mse.update(target, pred)
            mae.update(target, pred)

        producer.produce(topic_to, key="1", value=json.dumps(
            {"MSE": mse.get(), "MAE": mae.get()}
        ))
        producer.flush()


if __name__ == "__main__":
    online_learning_loop() 
    