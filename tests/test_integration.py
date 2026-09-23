"""
End-to-end style test for just the Kafka leg: produce one flight event and
confirm it can be consumed back. This does not require Spark or Postgres,
so it is fast and isolates Kafka connectivity problems.

Requires the Docker Kafka container to be running:
    docker compose up -d kafka
"""
import os
import sys
import json
import socket

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flight-events")


def _kafka_reachable():
    host, port = KAFKA_BOOTSTRAP_SERVERS.split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=3):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _kafka_reachable(), reason="Kafka is not reachable - start it with 'docker compose up -d'")
def test_produce_and_consume_roundtrip():
    from confluent_kafka import Producer, Consumer
    from producer.flight_producer import generate_flight_event

    event = generate_flight_event()

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(KAFKA_TOPIC, key=event["flight_id"], value=json.dumps(event))
    producer.flush(10)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "flight-integration-test",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([KAFKA_TOPIC])
    try:
        found = False
        for _ in range(20):  # up to ~20s
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            value = json.loads(msg.value().decode("utf-8"))
            if value.get("flight_id") == event["flight_id"]:
                found = True
                break
        assert found, "Produced event was not found back on the topic"
    finally:
        consumer.close()
