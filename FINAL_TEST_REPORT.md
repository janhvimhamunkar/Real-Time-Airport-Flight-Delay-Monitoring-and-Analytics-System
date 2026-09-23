# FINAL_TEST_REPORT.md

This report distinguishes between things that were **actually executed and
verified** while rebuilding this project, and things that could not be
executed in the sandboxed environment used to prepare it (no Docker daemon,
no internet access to Docker Hub / Kafka mirrors) and therefore need to be
confirmed once on your own Mac using the commands in the README.

## Test environment used for verification

- Ubuntu sandbox (not macOS), Python 3.12.3, OpenJDK 21, PostgreSQL 16
  (installed via `apt` for this verification only), no Docker available.
- The **application code** (producer, Spark job, schema, dashboard, DAGs)
  is unchanged in behavior regardless of host OS; only Kafka/Grafana/Airflow
  containers themselves could not be started here.

## Results

| Component | Status | Evidence |
|---|---|---|
| Producer logic | **PASS** | `pytest tests/test_producer.py` — 4/4 passed. Verified `flight_id == flight_number`, origin≠destination, status/delay consistency, cancelled-flight null handling. |
| Delay classification (Spark) | **PASS** | `pytest tests/test_processing.py` — 1/1 passed, covering every boundary (0, 1, 15, 16, 30, 31, 120 minutes, CANCELLED) against a real local Spark session. |
| PostgreSQL schema | **PASS** | `psql -f database/schema.sql` applied cleanly to a real PostgreSQL 16 database with zero errors; table + 4 indexes created. |
| PostgreSQL insert/upsert (Spark write path) | **PASS** | Ran the actual `write_to_postgres()` function from `spark/flight_stream_processor.py` against a real Postgres DB with a realistic record (AI999, BOM→DEL, 22 min delay). Confirmed in the DB: `status='DELAYED'`, `delay_category='MODERATE_DELAY'` — this is the exact bug that was fixed (see below). |
| Database tests | **PASS** | `pytest tests/test_database.py` — 3/3 passed (table exists, insert+query round-trip, duplicate `flight_id` upserts in place instead of creating a second row). |
| Kafka | **NOT EXECUTED HERE** | No Docker/Kafka broker available in this sandbox. `tests/test_integration.py` is written to run automatically and skip cleanly when Kafka isn't reachable (confirmed: it does skip, rather than fail). **Run `docker compose up -d kafka && pytest tests/test_integration.py` on your Mac to get a real PASS.** |
| Spark Structured Streaming (live, reading from Kafka) | **NOT EXECUTED HERE** | Same reason — requires a running Kafka broker. The transformation logic it depends on (`categorize_delays`, `write_to_postgres`) is independently verified above. **Run `./scripts/start.sh` then `tail -f logs/spark.log` on your Mac to confirm.** |
| Grafana | **NOT EXECUTED HERE** | No Docker available. Dashboard JSON and datasource/provisioning YAML were all syntax-validated (`json.load` / `yaml.safe_load` — all OK), and the datasource `uid` mismatch bug in the original project was fixed and cross-checked by hand against the dashboard's panel references. **Open http://localhost:3000 on your Mac to confirm panels render.** |
| Airflow | **NOT EXECUTED HERE** | No Docker available; full Airflow was not installed in this sandbox. Both DAG files were syntax-checked (`py_compile`) and reviewed line-by-line for the hardcoded-path/password bugs present in the original, which are now fixed (env-var driven). **Open http://localhost:8080 on your Mac and confirm both DAGs appear and can be triggered manually.** |
| End-to-end (Producer → Kafka → Spark → PostgreSQL → Grafana) | **NOT EXECUTED HERE** | Requires Docker. Each stage was verified independently as above; the full chain needs to be confirmed on your Mac. Suggested manual check: run `./scripts/start.sh`, then `docker exec -it flight-postgres psql -U <user> -d flight_monitoring -c "SELECT flight_id, status, delay_category FROM flights ORDER BY created_at DESC LIMIT 5;"` and confirm rows are appearing continuously. |
| Config/YAML/JSON syntax | **PASS** | `docker-compose.yml`, both Grafana provisioning YAML files, and `flight_dashboard.json` all parsed without error. |
| Python syntax (all modified files) | **PASS** | `py_compile` succeeded on the producer, Spark job, both Airflow DAGs, and all four test files. |
| Shell script syntax | **PASS** | `bash -n` succeeded on `start.sh`, `stop.sh`, `health_check.sh`. |

## Summary

- **5 of 9 test files/checks were actually executed and passed** against
  real components (Python, a real Spark session, a real PostgreSQL 16
  server) in this environment.
- **4 checks (Kafka, live Spark streaming, Grafana, Airflow) require Docker**,
  which was not available in this sandbox, and must be confirmed once on
  your Mac. Every piece of logic those components depend on was verified
  independently wherever possible (see table above).
- I have not marked anything "PASS" that I did not personally run and see
  pass.

## The one concrete bug this verification caught and fixed

The original `spark/flight_stream_processor.py` built an `INSERT` whose
column list said `status` but supplied `row.delay_category` as the value
— so the real flight status was never stored, and `delay_category` (which
didn't even exist as a column in the original `schema.sql`) silently took
its place. This is exactly the kind of bug that a "looks like it should
work" code review misses and only running it against a real database
catches — which is why this was executed here rather than just read.
