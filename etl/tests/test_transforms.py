"""
Tests for beam/transforms.py
  - validate(): plain Python unit tests (fast, no Beam)
  - CleanTrip:  runs in a small local Beam pipeline (TestPipeline)
Run from the etl/ folder:  pytest -v
"""
from datetime import datetime, timezone

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam.transforms import REJECTED, CleanTrip, validate


# SAMPLE DATA
# A fixture is a reusable piece of test data: pytest passes it into any test that lists it as an argument.
@pytest.fixture
def good_trip():
    return {
        "unique_key": "trip_1", "taxi_id": "taxi_9", "payment_type": "Credit Card",
        "trip_start_timestamp": "2022-01-01T08:00:00", "trip_seconds": 900,
        "trip_miles": 3.2, "fare": 14.5, "tips": 3.0,
        "pickup_community_area": 8, "dropoff_community_area": 32,
        "tolls": 0, "extras": 1, "company": "Flash Cab",
    }


# ---- validate(): unit tests ---------------------------------------------------
def test_valid_trip_has_no_problems(good_trip):
    assert validate(good_trip) == []


def test_zero_second_trip_is_bad_duration(good_trip):
    assert validate({**good_trip, "trip_seconds": 0}) == ["bad_duration"]


def test_missing_fare_is_bad_fare(good_trip):
    assert validate({**good_trip, "fare": None}) == ["bad_fare"]


def test_negative_tip_is_bad_tips(good_trip):
    assert validate({**good_trip, "tips": -1}) == ["bad_tips"]


def test_pickup_area_out_of_range(good_trip):
    assert validate({**good_trip, "pickup_community_area": 99}) == ["bad_pickup_area"]


def test_collects_all_problems_not_just_first(good_trip):
    bad = {**good_trip, "trip_seconds": 0, "fare": None, "trip_start_timestamp": None}
    assert set(validate(bad)) == {"missing_start_ts", "bad_duration", "bad_fare"}


def test_accepts_timezone_aware_datetime_from_bigquery(good_trip):
    # BigQuery returns real datetime objects with UTC attached, not strings
    ts = datetime(2022, 1, 1, 8, 0, tzinfo=timezone.utc)
    assert validate({**good_trip, "trip_start_timestamp": ts}) == []


def test_cash_trip_passes_validate(good_trip):
    # Payment type is a business filter handled in CleanTrip, not a data-quality rule
    assert validate({**good_trip, "payment_type": "Cash"}) == []


# CleanTrip: runs inside a real (local) Beam pipeline
def test_clean_trip_routes_rows_to_correct_outputs(good_trip):

    # create 3 dummy trip rows with specific issues
    cash = {**good_trip, "unique_key": "trip_2", "payment_type": "Cash"}
    zero_seconds = {**good_trip, "unique_key": "trip_3", "trip_seconds": 0}
    no_fare = {**good_trip, "unique_key": "trip_4", "fare": None}

    with TestPipeline() as p:

        results = (
            p
            | beam.Create([good_trip, cash, zero_seconds, no_fare])
            | beam.ParDo(CleanTrip()).with_outputs(REJECTED, main="valid")
        )

        # assert_that checks the PCollection's contents when the pipeline runs.
        # Each assert_that needs a unique label.
        valid_keys = results.valid | "ValidKeys" >> beam.Map(lambda r: r["unique_key"])
        assert_that(valid_keys, equal_to(["trip_1"]), label="CheckValid")

        rejected_keys = results[REJECTED] | "RejectedKeys" >> beam.Map(lambda r: r["unique_key"])
        assert_that(rejected_keys, equal_to(["trip_3", "trip_4"]), label="CheckRejected")
        # trip_2 (cash) appears in neither: it was silently filtered


def test_clean_record_has_expected_types(good_trip):
    def check(records):
        # assert_that also takes a function that receives the full list of outputs
        assert len(records) == 1
        r = records[0]
        assert isinstance(r["trip_start_ts"], datetime) and r["trip_start_ts"].tzinfo is None
        assert isinstance(r["trip_seconds"], int)
        assert isinstance(r["fare"], float)
        assert "payment_type" not in r   # only the columns we keep

    with TestPipeline() as p:
        results = (
            p
            | beam.Create([good_trip])
            | beam.ParDo(CleanTrip()).with_outputs(REJECTED, main="valid")
        )
        assert_that(results.valid, check)
        