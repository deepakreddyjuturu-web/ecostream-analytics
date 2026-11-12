import json
import time
import random
import hashlib
import uuid
import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KAFKA_BROKER = "localhost:9092"
TOPIC_RAW = "eco.sensor.telemetry"

STATIONS = [
    {"city": "Berlin", "station": "Station-Mitte", "zone": "Urban-Traffic"},
    {"city": "Hamburg", "station": "Station-Port", "zone": "Maritime-Industrial"},
    {"city": "Munich", "station": "Station-Park", "zone": "Residential"},
    {"city": "Frankfurt", "station": "Station-Airport", "zone": "Transport-Hub"},
    {"city": "Cologne", "station": "Station-Rhine", "zone": "Commercial"},
    {"city": "Stuttgart", "station": "Station-Neckar", "zone": "Industrial-Valley"}
]

def generate_event_id(station_name, timestamp, pm25, co2):
    raw_str = f"{station_name}-{timestamp:.2f}-{pm25}-{co2}-{uuid.uuid4().hex[:6]}"
    return hashlib.sha256(raw_str.encode("utf-8")).hexdigest()[:16]

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        api_version=(2, 0, 2),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1
    )
    logging.info("Sensor fleet online. Emitting telemetry with idempotency keys...")

    while True:
        station = random.choice(STATIONS)
        is_spike = random.random() < 0.35

        pm25 = round(random.uniform(55.0, 140.0) if is_spike else random.uniform(8.0, 30.0), 1)
        co2 = int(random.uniform(900, 1600) if is_spike else random.uniform(380, 600))
        temp = round(random.uniform(15.0, 25.0), 1)
        aqi = int(pm25 * 2.0 + (co2 / 45.0))
        now_ts = time.time()

        event_id = generate_event_id(station["station"], now_ts, pm25, co2)

        data = {
            "event_id": event_id,
            "timestamp": now_ts,
            "station": station["station"],
            "city": station["city"],
            "zone": station["zone"],
            "pm25": pm25,
            "co2": co2,
            "temperature_c": temp,
            "aqi": aqi
        }

        producer.send(TOPIC_RAW, key=event_id.encode("utf-8"), value=data)
        logging.info(f"Emitted [{event_id}] -> {data['city']} | AQI: {aqi} | PM2.5: {pm25}")
        time.sleep(0.5)

if __name__ == "__main__":
    run_producer()
