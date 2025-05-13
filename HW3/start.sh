#!/bin/bash

mkdir -p /app/data/bronze /app/data/silver /app/data/gold

mlflow ui --host 0.0.0.0 --port 5000 &

python /app/src/main.py

tail -f /dev/null