import json
import logging
from collections import deque
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KAFKA_BROKER = "localhost:9092"
TOPIC_IN = "eco.sensor.telemetry"
TOPIC_OUT = "eco.alerts.flagged"

SEEN_EVENTS = deque(maxlen=2000)
SEEN_SET = set()

def run_detector():
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BROKER,
        api_version=(2, 0, 2),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="ecostream-processor-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        api_version=(2, 0, 2),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    logging.info("Anomaly processor active. Idempotent deduplication enabled...")

    for message in consumer:
        rec = message.value
        event_id = rec.get("event_id")

        if event_id in SEEN_SET:
            logging.info(f"Duplicate event {event_id} filtered out by idempotency layer.")
            continue

        if event_id:
            if len(SEEN_EVENTS) >= 2000:
                oldest = SEEN_EVENTS.popleft()
                SEEN_SET.discard(oldest)
            SEEN_EVENTS.append(event_id)
            SEEN_SET.add(event_id)

        reasons = []
        if rec["pm25"] >= 50.0:
            reasons.append("HIGH_PM25")
        if rec["co2"] >= 900:
            reasons.append("HIGH_CO2")
        if rec["aqi"] >= 100:
            reasons.append("HAZARDOUS_AQI")

        if reasons:
            rec["alert_reasons"] = reasons
            rec["severity"] = "CRITICAL" if len(reasons) >= 2 else "WARNING"
            producer.send(TOPIC_OUT, key=event_id.encode("utf-8") if event_id else None, value=rec)
            logging.warning(f"ALERT [{event_id}]: {rec['city']} [{rec['severity']}] -> AQI: {rec['aqi']}")

if __name__ == "__main__":
    run_detector()
