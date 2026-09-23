import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from producer.flight_producer import generate_flight_event


def test_generate_flight_event_has_required_fields():
    event = generate_flight_event()
    for field in ["flight_id", "flight_number", "airline", "origin", "destination",
                  "status", "delay_minutes", "weather_condition", "timestamp"]:
        assert field in event


def test_flight_id_matches_flight_number():
    # flight_id must be the human flight number (e.g. "AI123"), not a
    # disconnected random id - this is what the DB uses as the natural key.
    event = generate_flight_event()
    assert event["flight_id"] == event["flight_number"]


def test_origin_and_destination_differ():
    for _ in range(50):
        event = generate_flight_event()
        assert event["origin"] != event["destination"]


def test_status_delay_consistency():
    for _ in range(200):
        event = generate_flight_event()
        if event["status"] == "ON_TIME":
            assert event["delay_minutes"] == 0
        elif event["status"] == "DELAYED":
            assert event["delay_minutes"] > 0
        elif event["status"] == "CANCELLED":
            assert event["actual_departure"] is None
            assert event["actual_arrival"] is None
