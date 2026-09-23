-- Flight monitoring schema.
-- Single table is enough for this project: Grafana reads straight from it.
--
-- flight_id is the human flight number (e.g. "AI123"). It is UNIQUE so that
-- re-sending the same flight (e.g. a status update, or a duplicate Kafka
-- message) UPDATEs the existing row instead of creating a duplicate one
-- (see the ON CONFLICT clause in spark/flight_stream_processor.py).

CREATE TABLE IF NOT EXISTS flights (
    id                   SERIAL PRIMARY KEY,
    flight_id            VARCHAR(20) NOT NULL UNIQUE,
    flight_number        VARCHAR(20) NOT NULL,
    airline              VARCHAR(50) NOT NULL,
    origin               VARCHAR(10) NOT NULL,
    destination          VARCHAR(10) NOT NULL,
    airport              VARCHAR(10) NOT NULL,
    scheduled_departure  TIMESTAMP,
    actual_departure     TIMESTAMP,
    scheduled_arrival    TIMESTAMP,
    actual_arrival       TIMESTAMP,
    status               VARCHAR(20) NOT NULL,
    delay_minutes        INT NOT NULL DEFAULT 0,
    delay_category       VARCHAR(20) NOT NULL,
    weather_condition    VARCHAR(50),
    event_timestamp      TIMESTAMP NOT NULL,
    created_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Only the indexes that the producer/Grafana queries actually need.
CREATE INDEX IF NOT EXISTS idx_flights_airport         ON flights(airport);
CREATE INDEX IF NOT EXISTS idx_flights_airline          ON flights(airline);
CREATE INDEX IF NOT EXISTS idx_flights_delay_category   ON flights(delay_category);
CREATE INDEX IF NOT EXISTS idx_flights_event_timestamp  ON flights(event_timestamp);
