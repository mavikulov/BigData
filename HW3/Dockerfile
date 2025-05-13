FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip \
    && pip3 install pyspark delta-spark mlflow pandas scikit-learn matplotlib

WORKDIR /app

COPY . /app

RUN chmod +x /app/start.sh

CMD ["bash", "/app/start.sh"]