from confluent_kafka import Consumer
import streamlit as st
import json
import time

st.set_page_config(page_title="Regression Task", layout="wide")

if "MSE" not in st.session_state:
    st.session_state["MSE"] = []
if "MAE" not in st.session_state:
    st.session_state["MAE"] = []

bootstrap_server = "localhost:9095,localhost:9097"
topic = "ml_topic"
config = {
    "bootstrap.servers" : bootstrap_server,
    "group.id": 'app_consumers'
}

consumer = Consumer(config)
consumer.subscribe([topic])

st.title("Мониторинг метрик модели CatBoostRegressor для данных Supestore Sales Data")
st.subheader("Онлайн-визуализация MSE и MAE")

chart_holder_mse = st.empty()
chart_holder_mae = st.empty()
column_mse = st.empty()
column_mae = st.empty()

def visualize():
    while True:
        message = consumer.poll(1)
        if message is not None:
            data_from_ml_producer = json.loads(message.value().decode("utf-8"))
            st.session_state["MSE"].append(data_from_ml_producer["MSE"])
            st.session_state["MAE"].append(data_from_ml_producer["MAE"])

            with chart_holder_mse.container():
                st.subheader("График MSE")
                st.line_chart(st.session_state["MSE"], height=300)

            with chart_holder_mae.container():
                st.subheader("График MAE")
                st.line_chart(st.session_state["MAE"], height=300)

            with column_mse:
                st.metric(label="Последнее значение MSE", value=round(st.session_state["MSE"][-1], 2))

            with column_mae:
                st.metric(label="Последнее значение MAE", value=round(st.session_state["MAE"][-1], 2))


if __name__ == "__main__":
    visualize()
