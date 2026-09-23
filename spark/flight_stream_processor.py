"""
Spark Structured Streaming job.

Kafka (flight-events) -> parse JSON -> validate -> classify delay -> PostgreSQL

Run with:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
      spark/flight_stream_processor.py
"""
import os
import time
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("flight_stream_processor")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flight-events")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "./spark-checkpoint")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "flight_monitoring")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Required fields. A record missing any of these is dropped as invalid.
REQUIRED_FIELDS = ["flight_id", "flight_number", "origin", "destination", "status", "timestamp"]

EVENT_SCHEMA = StructType([
    StructField("flight_id", StringType(), True),
    StructField("flight_number", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("airport", StringType(), True),
    StructField("scheduled_departure", TimestampType(), True),
    StructField("actual_departure", TimestampType(), True),
    StructField("scheduled_arrival", TimestampType(), True),
    StructField("actual_arrival", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("delay_minutes", IntegerType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("FlightDelayMonitor")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def categorize_delays(df: DataFrame) -> DataFrame:
    """
    Simple, documented delay classification:
      CANCELLED flights          -> CANCELLED
      0 minutes                  -> ON_TIME
      1-15 minutes                -> MINOR_DELAY
      16-30 minutes                -> MODERATE_DELAY
      31+ minutes                 -> MAJOR_DELAY
    """
    return df.withColumn(
        "delay_category",
        when(col("status") == "CANCELLED", "CANCELLED")
        .when(col("delay_minutes") == 0, "ON_TIME")
        .when((col("delay_minutes") >= 1) & (col("delay_minutes") <= 15), "MINOR_DELAY")
        .when((col("delay_minutes") >= 16) & (col("delay_minutes") <= 30), "MODERATE_DELAY")
        .when(col("delay_minutes") >= 31, "MAJOR_DELAY")
        .otherwise("UNKNOWN"),
    )


def write_to_postgres(batch_df: DataFrame, batch_id: int):
    """foreachBatch sink: upsert each micro-batch into PostgreSQL.

    ON CONFLICT (flight_id) DO UPDATE handles duplicate/repeated events for
    the same flight (e.g. a status changing from ON_TIME to DELAYED).
    """
    row_count = batch_df.count()
    if row_count == 0:
        log.info("Batch %s: no valid rows to write.", batch_id)
        return

    # Ensure no duplicate flight_ids in the same batch to avoid Postgres ON CONFLICT errors
    batch_df = batch_df.dropDuplicates(["flight_id"])
    rows = batch_df.collect()

    insert_query = """
        INSERT INTO flights (
            flight_id, flight_number, airline, origin, destination, airport,
            scheduled_departure, actual_departure, scheduled_arrival, actual_arrival,
            status, delay_minutes, delay_category, weather_condition, event_timestamp
        ) VALUES %s
        ON CONFLICT (flight_id) DO UPDATE SET
            status = EXCLUDED.status,
            delay_minutes = EXCLUDED.delay_minutes,
            delay_category = EXCLUDED.delay_category,
            actual_departure = EXCLUDED.actual_departure,
            actual_arrival = EXCLUDED.actual_arrival,
            weather_condition = EXCLUDED.weather_condition,
            event_timestamp = EXCLUDED.event_timestamp
    """
    values = [
        (
            row.flight_id, row.flight_number, row.airline, row.origin, row.destination, row.airport,
            row.scheduled_departure, row.actual_departure, row.scheduled_arrival, row.actual_arrival,
            row.status, row.delay_minutes, row.delay_category, row.weather_condition, row.timestamp,
        )
        for row in rows
    ]

    # A couple of retries: PostgreSQL can be briefly unavailable
    # (e.g. still starting up in Docker) without failing the whole stream.
    last_error = None
    for attempt in range(1, 4):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD, connect_timeout=5,
            )
            try:
                with conn:
                    with conn.cursor() as cur:
                        execute_values(cur, insert_query, values)
                log.info("Batch %s: wrote %s record(s) to PostgreSQL.", batch_id, len(values))
                return
            finally:
                conn.close()
        except psycopg2.OperationalError as e:
            last_error = e
            log.warning("Batch %s: PostgreSQL unavailable (attempt %s/3): %s", batch_id, attempt, e)
            time.sleep(2 * attempt)
        except Exception as e:
            log.error("Batch %s: failed to write to PostgreSQL: %s", batch_id, e)
            raise

    log.error("Batch %s: giving up after 3 attempts. Last error: %s", batch_id, last_error)
    raise last_error


def build_processed_stream(spark: SparkSession) -> DataFrame:
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_df.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    # Validation: drop malformed/incomplete records instead of crashing the
    # whole pipeline on one bad message. delay_minutes defaults to 0 when
    # missing/negative so classification never breaks.
    valid_df = (
        parsed_df.dropna(subset=REQUIRED_FIELDS)
        .withColumn(
            "delay_minutes",
            when(col("delay_minutes").isNull() | (col("delay_minutes") < 0), 0).otherwise(col("delay_minutes")),
        )
    )

    return categorize_delays(valid_df)


def process_stream(test_mode: bool = False):
    spark = create_spark_session()
    log.info("Spark session created. Subscribing to Kafka topic '%s' at %s", KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)

    categorized_df = build_processed_stream(spark)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    if test_mode:
        query = (
            categorized_df.writeStream.outputMode("append").format("console")
            .option("truncate", "false")
            .option("checkpointLocation", CHECKPOINT_DIR + "_console")
            .start()
        )
        query.awaitTermination(timeout=20)
        query.stop()
        log.info("Test stream processing completed.")
    else:
        query = (
            categorized_df.writeStream.outputMode("update")
            .foreachBatch(write_to_postgres)
            .option("checkpointLocation", CHECKPOINT_DIR)
            .start()
        )
        query.awaitTermination()


if __name__ == "__main__":
    try:
        process_stream(test_mode=False)
    except KeyboardInterrupt:
        log.info("Stopping Spark streaming job (Ctrl+C received)...")
    except Exception:
        log.exception("Spark streaming job failed")
        raise
