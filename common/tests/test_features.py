import pandas as pd
from taxi_features.features import build_features, create_label, FEATURE_COLUMNS, ID_COLUMNS


def make_trip(**overrides):
    row = {
        "unique_key": "a1",
        "trip_start_ts": "2022-01-08 23:15:00",   # Saturday
        "trip_seconds": 1200, "trip_miles": 5.0,
        "fare": 20.0, "tips": 5.0, "extras": 1.0, "tolls": 0.0,
        "pickup_community_area": 76, "dropoff_community_area": 32,
        "company": " Flash Cab ",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_output_columns():
    assert list(build_features(make_trip()).columns) == FEATURE_COLUMNS


def test_time_features():
    f = build_features(make_trip()).iloc[0]
    assert f["hour"] == 23 and f["day_of_week"] == 5 and f["is_weekend"] == 1


def test_airport_and_downtown():
    f = build_features(make_trip()).iloc[0]
    assert f["is_airport"] == 1 and f["is_downtown"] == 1 and f["same_area"] == 0


def test_zero_distance_and_duration_do_not_crash():
    f = build_features(make_trip(trip_miles=0.0, trip_seconds=0)).iloc[0]
    assert f["fare_per_mile"] == 0.0 and f["avg_speed_mph"] == 0.0


def test_missing_area_and_company():
    f = build_features(make_trip(pickup_community_area=None, company=None)).iloc[0]
    assert f["pickup_area"] == -1 and f["company"] == "unknown"


def test_label_threshold():
    assert create_label(make_trip(tips=5.0, fare=20.0)).iloc[0] == 1   # 25%
    assert create_label(make_trip(tips=4.0, fare=20.0)).iloc[0] == 0   # exactly 20% → not "more than"

def test_default_returns_only_features():
    assert list(build_features(make_trip()).columns) == FEATURE_COLUMNS


def test_keep_adds_id_columns_in_front():
    out = build_features(make_trip(), keep=ID_COLUMNS)
    assert list(out.columns) == ID_COLUMNS + FEATURE_COLUMNS
    assert out["unique_key"].iloc[0] == "a1"
    