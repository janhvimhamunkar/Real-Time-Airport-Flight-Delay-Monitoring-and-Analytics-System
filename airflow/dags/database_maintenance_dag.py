"""
Orchestration DAG: daily database maintenance (VACUUM ANALYZE) and a
plain-text analytics snapshot. Runs once a day, independent of the
continuous Spark streaming job.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

POSTGRES_HOST = os.environ.get("FLIGHT_POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("FLIGHT_POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("FLIGHT_POSTGRES_DB", "flight_monitoring")
POSTGRES_USER = os.environ.get("FLIGHT_POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("FLIGHT_POSTGRES_PASSWORD")

STATS_LOG_PATH = "/opt/airflow/logs/daily_stats.log"


def _connect():
    import psycopg2
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, connect_timeout=5,
    )


def run_vacuum_analyze():
    conn = _connect()
    try:
        conn.autocommit = True  # VACUUM cannot run inside a transaction block
        with conn.cursor() as cur:
            cur.execute("VACUUM ANALYZE flights;")
        print("VACUUM ANALYZE completed on flights table.")
    finally:
        conn.close()


def log_daily_statistics():
    conn = _connect()
    os.makedirs(os.path.dirname(STATS_LOG_PATH), exist_ok=True)
    try:
        with conn.cursor() as cur, open(STATS_LOG_PATH, "a") as f:
            f.write(f"\n--- Daily stats @ {datetime.utcnow().isoformat()} ---\n")

            cur.execute("""
                SELECT delay_category, COUNT(*) FROM flights GROUP BY delay_category ORDER BY 2 DESC;
            """)
            for category, count in cur.fetchall():
                f.write(f"{category}: {count}\n")

            cur.execute("""
                SELECT airline, ROUND(AVG(delay_minutes),1) FROM flights
                WHERE status != 'CANCELLED' GROUP BY airline ORDER BY 2 DESC;
            """)
            f.write("-- Average delay by airline --\n")
            for airline, avg_delay in cur.fetchall():
                f.write(f"{airline}: {avg_delay} min\n")
        print(f"Daily statistics written to {STATS_LOG_PATH}")
    finally:
        conn.close()


default_args = {
    "owner": "flight-monitor",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="database_maintenance_dag",
    description="Daily VACUUM ANALYZE and analytics snapshot for the flights table",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["maintenance"],
) as dag:

    vacuum_task = PythonOperator(task_id="run_vacuum_analyze", python_callable=run_vacuum_analyze)
    stats_task = PythonOperator(task_id="log_daily_statistics", python_callable=log_daily_statistics)

    vacuum_task >> stats_task
