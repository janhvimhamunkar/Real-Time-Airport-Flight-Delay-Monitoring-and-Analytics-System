"""
Flight event simulator.

Generates realistic, randomized flight events and publishes them as JSON
to the Kafka topic `flight-events`. This is the ONLY job of this file:
no business logic (delay classification, DB writes, etc.) belongs here -
that all happens downstream in Spark.
"""
import os
import json
import time
import random
import logging
from datetime import datetime, timedelta

from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("flight_producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flight-events")
INTERVAL = float(os.getenv("EVENT_GENERATION_INTERVAL", "1.0"))

AIRPORTS = ["BOM", "DEL", "BLR", "HYD", "MAA", "CCU"]
AIRLINES = {
    "Air India": "AI",
    "IndiGo": "6E",
    "SpiceJet": "SG",
    "Vistara": "UK",
    "Akasa Air": "QP",
    "AirAsia India": "I5",
}
WEATHER = ["Clear", "Rain", "Thunderstorm", "Fog", "Cloudy", "Windy"]
STATUS_CATEGORIES = ["ON_TIME", "DELAYED", "CANCELLED"]
STATUS_WEIGHTS = [60, 35, 5]


def generate_flight_event() -> dict:
    """Build one realistic, self-consistent flight event as a dict."""
    origin = random.choice(AIRPORTS)
    destination = random.choice([a for a in AIRPORTS if a != origin])

    airline = random.choice(list(AIRLINES.keys()))
    flight_number = f"{AIRLINES[airline]}{random.randint(100, 999)}"

    now = datetime.now()
    scheduled_departure = now - timedelta(minutes=random.randint(0, 60))
    scheduled_arrival = scheduled_departure + timedelta(minutes=random.randint(60, 180))

    status = random.choices(STATUS_CATEGORIES, weights=STATUS_WEIGHTS)[0]

    delay_minutes = 0
    actual_departure = scheduled_departure
    actual_arrival = scheduled_arrival

    if status == "DELAYED":
        delay_minutes = random.randint(1, 120)
        actual_departure = scheduled_departure + timedelta(minutes=delay_minutes)
        actual_arrival = scheduled_arrival + timedelta(minutes=delay_minutes)
    elif status == "CANCELLED":
        delay_minutes = 0
        actual_departure = None
        actual_arrival = None

    return {
        # flight_id doubles as the human flight number (e.g. "AI123"), and
        # is what the database uses to detect/merge duplicate events.
        "flight_id": flight_number,
        "flight_number": flight_number,
        "airline": airline,
        "origin": origin,
        "destination": destination,
        "airport": origin,
        "scheduled_departure": scheduled_departure.isoformat(),
        "actual_departure": actual_departure.isoformat() if actual_departure else None,
        "scheduled_arrival": scheduled_arrival.isoformat(),
        "actual_arrival": actual_arrival.isoformat() if actual_arrival else None,
        "status": status,
        "delay_minutes": delay_minutes,
        "weather_condition": random.choice(WEATHER),
        "timestamp": now.isoformat(),
    }


def delivery_report(err, msg):
    """Kafka delivery callback. Errors are always logged, never swallowed."""
    if err is not None:
        log.error("Delivery failed for record %s: %s", msg.key(), err)
    else:
        log.debug("Delivered to %s [partition %s]", msg.topic(), msg.partition())


def run_producer(num_events: int | None = None):
    conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)
    log.info("Starting flight producer -> %s (topic: %s)", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    events_sent = 0
    try:
        while True:
            event = generate_flight_event()
            try:
                producer.produce(
                    KAFKA_TOPIC,
                    key=event["flight_id"],
                    value=json.dumps(event),
                    callback=delivery_report,
                )
                producer.poll(0)
            except BufferError:
                log.warning("Local producer queue is full, waiting for delivery reports...")
                producer.poll(1)
                continue

            log.info(
                "Produced %s | %s -> %s | status=%s delay=%smin",
                event["flight_number"], event["origin"], event["destination"],
                event["status"], event["delay_minutes"],
            )

            events_sent += 1
            if num_events and events_sent >= num_events:
                break
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        log.info("Stopping producer (Ctrl+C received)...")
    except Exception:
        log.exception("Producer crashed unexpectedly")
        raise
    finally:
        producer.flush(10)
        log.info("Producer stopped. Total events sent: %s", events_sent)


if __name__ == "__main__":
    run_producer()
