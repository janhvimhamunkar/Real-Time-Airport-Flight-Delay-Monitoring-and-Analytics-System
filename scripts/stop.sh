#!/bin/bash
# Stops the producer and Spark job (started by start.sh), then the Docker infra.
cd "$(dirname "$0")/.."

for name in producer spark; do
    pidfile="logs/${name}.pid"
    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            echo "==> Stopping $name (pid $pid)..."
            kill "$pid" 2>/dev/null || true
        fi
        rm -f "$pidfile"
    fi
done

echo "==> Stopping Docker infrastructure..."
docker compose down

echo "==> Stopped. (Data in PostgreSQL/Grafana/Airflow volumes is preserved;"
echo "    run 'docker compose down -v' instead to wipe it completely.)"
