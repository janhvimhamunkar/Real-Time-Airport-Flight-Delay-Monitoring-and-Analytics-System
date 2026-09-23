# Real-Time Airport Flight Delay Monitoring and Analytics System

A college-project-scale real-time data pipeline: a Python simulator generates
flight events, Kafka streams them, Spark Structured Streaming classifies
delays and writes to PostgreSQL, Grafana visualizes the results in real
time, and Airflow runs scheduled health checks / maintenance alongside it.

## 1. Architecture

```
Flight Simulator
      |
      | JSON flight events
      v
Apache Kafka  (topic: flight-events)
      |
      | real-time stream
      v
Spark Structured Streaming
      |
      | processed flight data (delay classified)
      v
PostgreSQL   (table: flights)
      |
      | SQL queries
      v
Grafana
      |
      v
Real-Time Flight Delay Dashboard
```

Apache Airflow runs **alongside** this pipeline (it does not sit in the
data path) and performs:
- hourly Kafka + PostgreSQL health checks (`flight_monitoring_dag`)
- daily `VACUUM ANALYZE` + an analytics snapshot (`database_maintenance_dag`)

## 2. Workflow

1. `producer/flight_producer.py` generates a realistic flight event
   (flight number, route, scheduled/actual times, weather) as JSON and
   publishes it to the Kafka topic `flight-events`.
2. Spark (`spark/flight_stream_processor.py`) consumes the topic, parses
   and validates the JSON, classifies the delay, and **upserts** the row
   into PostgreSQL (`ON CONFLICT (flight_id) DO UPDATE`), so a repeated
   event for the same flight updates it instead of duplicating it.
3. Delay classification (documented in the code):
   - `CANCELLED` flights → `CANCELLED`
   - `0` minutes → `ON_TIME`
   - `1-15` minutes → `MINOR_DELAY`
   - `16-30` minutes → `MODERATE_DELAY`
   - `31+` minutes → `MAJOR_DELAY`
4. Grafana queries PostgreSQL directly, refreshing every 5 seconds.

## 3. Technologies

Apache Kafka (KRaft mode, no ZooKeeper) · Apache Spark Structured Streaming
(PySpark) · PostgreSQL 16 · Grafana 11 · Apache Airflow 2.10 · Python 3.11.

## 4. Why this architecture (Docker vs native)

Kafka, PostgreSQL, Grafana, and Airflow run in **Docker** (one container
each, one instance each) because native Homebrew installs of Kafka and
Airflow are fragile on macOS (Airflow's scheduler in particular has known
multiprocessing issues on macOS). The **producer and the Spark job run as
local Python processes** on your Mac, because `pip install pyspark`
already bundles Spark itself — there is no need to containerize or
separately install Spark, and running it locally makes it trivial to see
logs and restart it while developing.

> **Prerequisite:** Docker Desktop must be installed and running. If your
> Mac does not have Docker installed yet: `brew install --cask docker`,
> then open Docker Desktop once so it finishes initializing before you
> run `docker compose up -d`.

## 5. Prerequisites (macOS, Apple Silicon)

| Tool | Version used | Notes |
|---|---|---|
| Docker Desktop | latest | Apple Silicon native |
| Python | 3.11.x | PySpark 3.5.x does not officially support 3.13 yet; 3.11 is the safe choice |
| Java (for local PySpark) | 17 (OpenJDK, `brew install openjdk@17`) | Needed because the producer doesn't need Java, but the Spark job does |

Install Python 3.11 if needed: `brew install python@3.11`

## 6. Installation

```bash
cd RealTimeFlightDelayMonitor
cp .env.example .env          # edit passwords if you like
```

## 7. Environment variables (`.env`)

| Variable | Purpose |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka address as seen from your Mac (`localhost:9092`) |
| `KAFKA_TOPIC` | Kafka topic name (`flight-events`) |
| `POSTGRES_HOST/PORT/DB/USER/PASSWORD` | PostgreSQL connection used by the producer's DB, Spark, Grafana, Airflow |
| `GRAFANA_PORT` / `GRAFANA_ADMIN_PASSWORD` | Grafana web UI |
| `AIRFLOW_PORT` / `AIRFLOW_ADMIN_PASSWORD` | Airflow web UI |
| `EVENT_GENERATION_INTERVAL` | Seconds between simulated flight events |

`.env` is git-ignored; only `.env.example` (placeholder values) is committed.

## 8. Database setup

The `flights` table is created **automatically** the first time the
PostgreSQL container starts, from `database/schema.sql`
(mounted into `/docker-entrypoint-initdb.d/`). A second database, `airflow`,
is created the same way for Airflow's own metadata — this is still a
**single PostgreSQL instance**, just two databases inside it.

## 9. Kafka setup

Kafka runs in KRaft mode (no ZooKeeper) via the official `apache/kafka`
image, which is multi-arch and runs natively on Apple Silicon. The topic
`flight-events` is auto-created on first use.

## 10. Spark setup

No separate Spark installation is required — `pip install pyspark` (see
`requirements.txt`) bundles Spark. The job downloads the small
`spark-sql-kafka` connector jar the first time it runs (via
`spark.jars.packages`), which needs network access once.

## 11. Airflow setup

Airflow runs as a single Docker container (`airflow standalone`, using
`LocalExecutor` against the shared PostgreSQL instance). This avoids
native macOS scheduler/multiprocessing problems entirely. DAGs live in
`airflow/dags/` and are mounted straight into the container — no build
step needed.

## 12. Grafana setup

Grafana's PostgreSQL datasource and the "Real-Time Flight Delay
Monitoring" dashboard are both provisioned automatically from
`grafana/provisioning/` and `grafana/dashboards/` — nothing to click
through manually.

## 13. How to start

```bash
docker compose up -d          # Kafka, PostgreSQL, Grafana, Airflow
./scripts/start.sh            # venv + producer + Spark job (all local)
```

`start.sh` also runs `docker compose up -d` itself, so on a fresh machine
you can just run:

```bash
./scripts/start.sh
```

## 14. How to verify

```bash
./scripts/health_check.sh
```

Expected output once everything is up:
```
[*] Kafka
  [OK]    Kafka is reachable on port 9092
[*] PostgreSQL
  [OK]    PostgreSQL is reachable on port 5433
  [OK]    Can query the 'flight_monitoring' database
[*] Grafana
  [OK]    Grafana is reachable on port 3000
[*] Airflow
  [OK]    Airflow is reachable on port 8080
[*] Spark streaming job (local process)
  [OK]    Spark job is running (pid ...)
[*] Producer (local process)
  [OK]    Producer is running (pid ...)
```

Then open:
- **Grafana dashboard:** http://localhost:3000 (user `admin`, password from `.env`)
- **Airflow UI:** http://localhost:8080 (user `admin`, password from `.env`)

Watch rows accumulate: `tail -f logs/producer.log` and `tail -f logs/spark.log`.

## 15. How to stop

```bash
./scripts/stop.sh
```
This stops the producer and Spark job, then `docker compose down`
(container data in named volumes is preserved; add `-v` to wipe it).

## 16. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `docker compose up` fails to pull images | Docker Desktop not running, or no internet access |
| Spark job exits immediately with a Kafka connection error | Kafka container not healthy yet - wait ~15s after `docker compose up -d` and retry |
| Producer runs but nothing appears in Grafana | Check `logs/spark.log` - the Spark job must be running and connected for data to reach PostgreSQL |
| Grafana panels show "datasource not found" | Restart the `grafana` container so it re-reads `grafana/provisioning/` |
| `psql`/health check auth fails | `.env` password doesn't match what's in the Postgres container - delete the `postgres_data` volume (`docker compose down -v`) and start again |
| Port already in use (5433/9092/3000/8080) | Something else on your Mac is already using that port - stop it, or change the port in `.env` |

## 17. Example output

Producer log:
```
2026-09-19 ... [INFO] Produced AI123 | BOM -> DEL | status=DELAYED delay=22min
```

Row in PostgreSQL:
```
flight_id | airline   | origin | destination | status  | delay_minutes | delay_category
AI123     | Air India | BOM    | DEL         | DELAYED | 22             | MODERATE_DELAY
```

## Project structure

```
RealTimeFlightDelayMonitor/
├── README.md
├── FINAL_TEST_REPORT.md
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── producer/flight_producer.py
├── spark/flight_stream_processor.py
├── database/
│   ├── schema.sql
│   ├── queries.sql
│   └── init-airflow-db.sh
├── airflow/dags/
│   ├── flight_monitoring_dag.py
│   └── database_maintenance_dag.py
├── grafana/
│   ├── dashboards/flight_dashboard.json
│   └── provisioning/{datasources,dashboards}/*.yml
├── scripts/{start.sh,stop.sh,health_check.sh}
└── tests/{test_producer.py,test_processing.py,test_database.py,test_integration.py}
```
