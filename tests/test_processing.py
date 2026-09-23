import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from pyspark.sql import SparkSession
from spark.flight_stream_processor import categorize_delays


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("TestFlightProcessor")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def test_categorize_delays(spark):
    data = [
        {"flight_id": "1", "status": "ON_TIME", "delay_minutes": 0},
        {"flight_id": "2", "status": "DELAYED", "delay_minutes": 1},
        {"flight_id": "3", "status": "DELAYED", "delay_minutes": 15},
        {"flight_id": "4", "status": "DELAYED", "delay_minutes": 16},
        {"flight_id": "5", "status": "DELAYED", "delay_minutes": 30},
        {"flight_id": "6", "status": "DELAYED", "delay_minutes": 31},
        {"flight_id": "7", "status": "DELAYED", "delay_minutes": 120},
        {"flight_id": "8", "status": "CANCELLED", "delay_minutes": 0},
    ]
    df = spark.createDataFrame(data)
    results = {row["flight_id"]: row["delay_category"] for row in categorize_delays(df).collect()}

    assert results["1"] == "ON_TIME"
    assert results["2"] == "MINOR_DELAY"
    assert results["3"] == "MINOR_DELAY"
    assert results["4"] == "MODERATE_DELAY"
    assert results["5"] == "MODERATE_DELAY"
    assert results["6"] == "MAJOR_DELAY"
    assert results["7"] == "MAJOR_DELAY"
    assert results["8"] == "CANCELLED"
