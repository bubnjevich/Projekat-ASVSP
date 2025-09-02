#!/usr/bin/python3
import os
import time
import json
import requests
import pandas as pd
from kafka import KafkaProducer
import kafka.errors

# Environment variables
API_KEY = os.environ.get("WEATHER_API_KEY")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "broker1:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "weather_raw")
CSV_FILE = os.environ.get("AIRPORTS_CSV", "airports.csv")

def get_weather(lat, lon):
    url = f"http://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={lat},{lon}&days=1&aqi=no&alerts=no"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

# Load airports from CSV
airports_df = pd.read_csv(CSV_FILE)

# Connect to Kafka
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Connected to Kafka")
        break
    except kafka.errors.NoBrokersAvailable:
        print("Kafka not available, retrying...")
        time.sleep(3)

# Loop – šalje podatke na svake 2 minute
while True:
    for _, row in airports_df.iterrows():
        try:
            data = get_weather(row["location_lat"], row["location_lng"])
            # enrich sa CSV kolonama
            data["airport_code"] = row["airport_code"]
            data["state"] = row["state"]
            data["county"] = row["county"]
            data["timezone"] = row["timezone"]

            producer.send(TOPIC, value=data)
            print(f"Sent weather for {row['airport_code']} to topic {TOPIC}")
            time.sleep(2)
        except Exception as e:
            print(f"Error fetching {row['airport_code']}: {e}")

