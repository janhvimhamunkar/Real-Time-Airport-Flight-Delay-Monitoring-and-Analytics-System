#!/bin/bash
cd "$(dirname "$0")/.."
[ -f .env ] && { set -a; source .env; set +a; }

echo "======================================"
echo "  Flight Delay Monitor - Health Check"
echo "======================================"

check_port() {
    local name=$1 port=$2
    if nc -z localhost "$port" 2>/dev/null; then
        echo "  [OK]    $name is reachable on port $port"
        return 0
    else
        echo "  [ERROR] $name is NOT reachable on port $port"
        return 1
    fi
}

echo "[*] Kafka"
check_port "Kafka" 9092

echo "[*] PostgreSQL"
if check_port "PostgreSQL" "${POSTGRES_PORT:-5432}"; then
    if PGPASSWORD="${POSTGRES_PASSWORD}" psql -U "${POSTGRES_USER}" -h localhost -p "${POSTGRES_PORT:-5432}" \
        -d "${POSTGRES_DB}" -c "SELECT 1;" >/dev/null 2>&1; then
        echo "  [OK]    Can query the '${POSTGRES_DB}' database"
    else
        echo "  [ERROR] Port is open but the query failed (check .env credentials)"
    fi
fi

echo "[*] Grafana"
check_port "Grafana" "${GRAFANA_PORT:-3000}"

echo "[*] Airflow"
check_port "Airflow" "${AIRFLOW_PORT:-8080}"

echo "[*] Spark streaming job (local process)"
if [ -f logs/spark.pid ] && kill -0 "$(cat logs/spark.pid)" 2>/dev/null; then
    echo "  [OK]    Spark job is running (pid $(cat logs/spark.pid))"
else
    echo "  [WARNING] Spark job process not found. Start it with ./scripts/start.sh"
fi

echo "[*] Producer (local process)"
if [ -f logs/producer.pid ] && kill -0 "$(cat logs/producer.pid)" 2>/dev/null; then
    echo "  [OK]    Producer is running (pid $(cat logs/producer.pid))"
else
    echo "  [WARNING] Producer process not found. Start it with ./scripts/start.sh"
fi

echo "======================================"
