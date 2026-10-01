from datetime import date, datetime
from beam_etl.sources import landing_paths, live_to_raw, merge_trip


def test_landing_paths_one_per_day_end_exclusive():
    paths = landing_paths("gs://lake", "live_trips", date(2022, 4, 1), date(2022, 4, 3))
    assert paths == ["gs://lake/landing/live_trips/event_date=2022-04-01/*.json",
                     "gs://lake/landing/live_trips/event_date=2022-04-02/*.json"]


def test_live_to_raw_converts_names_and_types():
    row = live_to_raw({"unique_key": "a1", "trip_start_timestamp": "2022-04-01 08:30:00 UTC",
                       "trip_seconds": "600", "fare": 12.5, "pickup_community_area": "8"})
    assert row["trip_start_ts"] == datetime(2022, 4, 1, 8, 30)
    assert row["trip_seconds"] == 600 and row["pickup_community_area"] == 8 and row["fare"] == 12.5


def test_merge_trip_skips_when_outcome_missing():
    assert list(merge_trip(("a1", {"request": [{"unique_key": "a1"}], "outcome": []}))) == []


def test_merge_trip_adds_tips():
    out = list(merge_trip(("a1", {"request": [{"unique_key": "a1"}], "outcome": [{"tips": 3.0}]})))
    assert out == [{"unique_key": "a1", "tips": 3.0}]
    