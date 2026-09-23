import os
import sys
import datetime
import pytest
import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "flight_monitoring")
POSTGRES_USER = os.getenv("POSTGRES_USER", "flightapp")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me")


def _get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, connect_timeout=3,
    )


@pytest.fixture
def conn():
    try:
        connection = _get_connection()
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL is not reachable - start it with 'docker compose up -d' to run this test.")
    yield connection
    connection.close()


def test_schema_has_flights_table(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.flights');")
        assert cur.fetchone()[0] == "flights"


def test_insert_and_query_flight(conn):
    flight_id = f"TEST{int(datetime.datetime.now().timestamp())}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO flights (
                flight_id, flight_number, airline, origin, destination, airport,
                status, delay_minutes, delay_category, event_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (flight_id, flight_id, "Air India", "BOM", "DEL", "BOM",
             "DELAYED", 22, "MODERATE_DELAY", datetime.datetime.now()),
        )
        conn.commit()

        cur.execute("SELECT delay_category, delay_minutes FROM flights WHERE flight_id = %s", (flight_id,))
        row = cur.fetchone()
        assert row == ("MODERATE_DELAY", 22)

        # Cleanup so repeated test runs stay idempotent.
        cur.execute("DELETE FROM flights WHERE flight_id = %s", (flight_id,))
        conn.commit()


def test_duplicate_flight_id_is_upserted(conn):
    flight_id = f"DUPTEST{int(datetime.datetime.now().timestamp())}"
    upsert = """
        INSERT INTO flights (
            flight_id, flight_number, airline, origin, destination, airport,
            status, delay_minutes, delay_category, event_timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (flight_id) DO UPDATE SET
            status = EXCLUDED.status, delay_minutes = EXCLUDED.delay_minutes,
            delay_category = EXCLUDED.delay_category
    """
    with conn.cursor() as cur:
        cur.execute(upsert, (flight_id, flight_id, "IndiGo", "DEL", "BOM", "DEL",
                              "ON_TIME", 0, "ON_TIME", datetime.datetime.now()))
        cur.execute(upsert, (flight_id, flight_id, "IndiGo", "DEL", "BOM", "DEL",
                              "DELAYED", 45, "MAJOR_DELAY", datetime.datetime.now()))
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM flights WHERE flight_id = %s", (flight_id,))
        assert cur.fetchone()[0] == 1  # updated in place, not duplicated

        cur.execute("SELECT delay_category FROM flights WHERE flight_id = %s", (flight_id,))
        assert cur.fetchone()[0] == "MAJOR_DELAY"

        cur.execute("DELETE FROM flights WHERE flight_id = %s", (flight_id,))
        conn.commit()
