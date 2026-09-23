-- Reference queries. These are the same queries the Grafana dashboard
-- panels use; kept here so they can be run by hand (psql) or by the
-- Airflow daily maintenance DAG.

-- Average delay by airline (excludes cancelled flights)
SELECT airline, ROUND(AVG(delay_minutes), 1) AS avg_delay_minutes
FROM flights
WHERE status != 'CANCELLED'
GROUP BY airline
ORDER BY avg_delay_minutes DESC;

-- Average delay by airport
SELECT airport, ROUND(AVG(delay_minutes), 1) AS avg_delay_minutes
FROM flights
WHERE status != 'CANCELLED'
GROUP BY airport
ORDER BY avg_delay_minutes DESC
LIMIT 10;

-- Flight status distribution
SELECT delay_category, COUNT(*) AS flights
FROM flights
GROUP BY delay_category
ORDER BY flights DESC;

-- On-time percentage
SELECT
    ROUND(100.0 * COUNT(*) FILTER (WHERE delay_category = 'ON_TIME') / NULLIF(COUNT(*), 0), 1) AS on_time_pct
FROM flights;
