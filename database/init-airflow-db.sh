#!/bin/sh
# Runs once, automatically, the first time the Postgres container starts
# (via docker-entrypoint-initdb.d). Creates a second database, "airflow",
# inside the SAME PostgreSQL instance so we only ever run one Postgres.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE airflow OWNER ${POSTGRES_USER}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
EOSQL
