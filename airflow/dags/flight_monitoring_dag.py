"""
Orchestration DAG (NOT part of the real-time Kafka -> Spark -> PostgreSQL
path). Runs hourly to confirm the streaming infrastructure is healthy and
writes a one-line status report. All connection details come from
environment variables set in docker-compose.yml (no hardcoded hosts,
paths, or passwords).
"""
import os
import socket
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("FLIGHT_KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
POSTGRES_HOST = os.environ.get("FLIGHT_POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("FLIGHT_POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("FLIGHT_POSTGRES_DB", "flight_monitoring")
POSTGRES_USER = os.environ.get("FLIGHT_POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("FLIGHT_POSTGRES_PASSWORD")

REPORT_PATH = "/opt/airflow/logs/monitoring_report.txt"


def check_kafka():
    host, port = KAFKA_BOOTSTRAP_SERVERS.split(":")
    with socket.create_connection((host, int(port)), timeout=5):
        pass
    print(f"Kafka reachable at {KAFKA_BOOTSTRAP_SERVERS}")


def check_postgres():
    import psycopg2
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, connect_timeout=5,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flights;")
            count = cur.fetchone()[0]
        print(f"PostgreSQL reachable. flights table has {count} rows.")
    finally:
        conn.close()


def write_report(**context):
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "a") as f:
        f.write(f"[{datetime.utcnow().isoformat()}] Health check OK (Kafka + PostgreSQL reachable)\n")


default_args = {
    "owner": "flight-monitor",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flight_monitoring_dag",
    description="Hourly health checks for Kafka and PostgreSQL",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["monitoring"],
) as dag:

    check_kafka_task = PythonOperator(task_id="check_kafka_health", python_callable=check_kafka)
    check_postgres_task = PythonOperator(task_id="check_postgres_health", python_callable=check_postgres)
    report_task = PythonOperator(task_id="write_monitoring_report", python_callable=write_report)

    [check_kafka_task, check_postgres_task] >> report_task
