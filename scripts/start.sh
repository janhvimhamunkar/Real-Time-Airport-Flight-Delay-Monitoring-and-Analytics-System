#!/bin/bash
# Starts the whole project:
#   1. Docker infra: Kafka, PostgreSQL, Grafana, Airflow
#   2. Local Python venv (producer + Spark job run natively - simplest/most
#      reliable option on Apple Silicon, see README)
#   3. Producer and Spark job, both in the background, logging to ./logs/
set -e
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
    echo "No .env found, copying .env.example -> .env"
    cp .env.example .env
fi
set -a; source .env; set +a

mkdir -p logs

echo "==> Starting Docker infrastructure (Kafka, PostgreSQL, Grafana, Airflow)..."
docker compose up -d

echo "==> Waiting for Kafka and PostgreSQL to become healthy..."
for i in $(seq 1 30); do
    KAFKA_STATE=$(docker inspect -f '{{.State.Health.Status}}' flight-kafka 2>/dev/null || echo "starting")
    PG_STATE=$(docker inspect -f '{{.State.Health.Status}}' flight-postgres 2>/dev/null || echo "starting")
    if [ "$KAFKA_STATE" = "healthy" ] && [ "$PG_STATE" = "healthy" ]; then
        echo "    Kafka and PostgreSQL are healthy."
        break
    fi
    sleep 2
done

echo "==> Setting up local Python environment for the producer + Spark job..."
if [ ! -d venv ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install -q -r requirements.txt

echo "==> Starting the flight event producer (background, logs/producer.log)..."
nohup python3 producer/flight_producer.py > logs/producer.log 2>&1 &
echo $! > logs/producer.pid

echo "==> Starting the Spark Structured Streaming job (background, logs/spark.log)..."
nohup python3 spark/flight_stream_processor.py > logs/spark.log 2>&1 &
echo $! > logs/spark.pid

echo ""
echo "==> All components started."
echo "    Grafana:  http://localhost:${GRAFANA_PORT:-3000}  (user: admin / password: from .env)"
echo "    Airflow:  http://localhost:${AIRFLOW_PORT:-8080}  (user: admin / password: from .env)"
echo "    Producer log: tail -f logs/producer.log"
echo "    Spark log:    tail -f logs/spark.log"
echo ""
echo "Run ./scripts/health_check.sh to verify everything is running."
