#!/usr/bin/python3
import os
import time
import json
import requests
import pandas as pd
from kafka import KafkaProducer
import kafka.errors
import datetime

# Environment variables
API_KEY = os.environ.get("WEATHER_API_KEY")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "broker1:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "weather_raw")
CSV_FILE = os.environ.get("AIRPORTS_CSV", "airports.csv")
ROW_LIMIT = 5 # None za ceo fajl

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

# Loop – šalje podatke na svake 2 sekunde
while True:
    df_iter = airports_df.head(ROW_LIMIT) if ROW_LIMIT else airports_df
    for _, row in df_iter.iterrows():
        try:
            weather = get_weather(row["location_lat"], row["location_lng"])
            

            # izdvajanje meteo podataka
            current = weather.get("current", {})
            forecast = weather.get("forecast", {}).get("forecastday", [{}])[0]
            

            data = {
                "airport_code": row["airport_code"],
                "state": row["state"],
                "county": row["county"],
                "timezone": row["timezone"],
                # dodatne kolone koje obogacuju batch
                "start_time_utc": datetime.datetime.utcnow().isoformat(),
                "precip_in": current.get("precip_in"),
                "temp_c": current.get("temp_c"),
                "humidity": current.get("humidity"),
                "cloud": current.get("cloud"),  # oblacnost u procentima
                "wind_kph": current.get("wind_kph"),   # jacina vetra
                "pressure_mb" : current.get("pressure_mb"), # pritisak u milibarima
                "vis_km": current.get("vis_km"), # vidljivost u km
                "dewpoint_c" : current.get("dewpoint_c"), # tacka rose, tj. temperatura na kojoj vazduh mora da se ohladi da bi dobio zasicenje
                "uv" : current.get("uv"), # uv indeks zracenja
                "gust_kph" : current.get("gust_kph"), # udar vetra u km/h
                "is_day" : current.get("is_day"),
                "gti" : current.get("gti") # kolocina Suncevog zracenja (direkno + difuzno + refkletno)
            }

            producer.send(TOPIC, value=data)
            print(f"Sent weather for {row['airport_code']} to topic {TOPIC}")
            time.sleep(2)
        except Exception as e:
            print(f"Error fetching {row['airport_code']}: {e}")

