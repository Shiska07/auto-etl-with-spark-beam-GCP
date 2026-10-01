"""
sources.py
How raw trips are read for each source. Every reader returns a PCollection of dicts
with the SAME field names and types, so the rest of the pipeline doesn't care where
the data came from.

  historical: BigQuery public table via SQL (sql_queries/02_load_table.sql)
  live:       landing JSON files (requests + outcomes), joined on unique_key
"""

import json
from datetime import date, datetime, timedelta
from pathlib import Path

import apache_beam as beam

SQL_DIR = Path(__file__).resolve().parent.parent / "sql_queries"
HISTORICAL_QUERY = "02_load_table.sql"

# Outcomes (tips) can land a little after the trip; read this many extra days of them
OUTCOME_GRACE_DAYS = 1


# ----------------------------- HISTORICAL ------------------------------------

def read_historical(p, args):
    """Run the extraction query for [start_date, end_date) against BigQuery."""
    query = (SQL_DIR / HISTORICAL_QUERY).read_text().format(
        start_date=args.start_date, end_date=args.end_date)
    return p | "ReadBigQuery" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)


# ----------------------------- LIVE ------------------------------------------

def landing_paths(lake: str, folder: str, start: date, end: date) -> list[str]:
    """One glob per day in [start, end), e.g. …/landing/live_trips/event_date=2022-04-01/*.json"""
    paths, d = [], start
    while d < end:
        paths.append(f"{lake}/landing/{folder}/event_date={d}/*.json")
        d += timedelta(days=1)
    return paths


def _to_int(v):
    return None if v in (None, "") else int(float(v))     # BigQuery exports INT64 as JSON strings


def _to_float(v):
    return None if v in (None, "") else float(v)


def _to_ts(v):
    """'2022-04-01 00:15:00 UTC' → naive UTC datetime (same as the silver timestamp type)."""
    if not v:
        return None
    return datetime.fromisoformat(v.replace(" UTC", "").replace("T", " ").rstrip("Z"))


def live_to_raw(row: dict) -> dict:
    """Convert a merged landing record to the same field names/types the historical query returns."""
    return {
        "unique_key": row.get("unique_key"),
        "taxi_id": row.get("taxi_id"),
        "trip_start_ts": _to_ts(row.get("trip_start_timestamp")),
        "trip_seconds": _to_int(row.get("trip_seconds")),
        "trip_miles": _to_float(row.get("trip_miles")),
        "fare": _to_float(row.get("fare")),
        "tips": _to_float(row.get("tips")),
        "pickup_community_area": _to_int(row.get("pickup_community_area")),
        "dropoff_community_area": _to_int(row.get("dropoff_community_area")),
        "tolls": _to_float(row.get("tolls")),
        "extras": _to_float(row.get("extras")),
        "company": row.get("company"),
        "payment_type": row.get("payment_type"),
    }


def merge_trip(keyed):
    """Join one request with its outcome. Skip trips whose tip hasn't arrived yet."""
    _key, grouped = keyed
    requests, outcomes = list(grouped["request"]), list(grouped["outcome"])
    if not requests or not outcomes:
        return
    row = dict(requests[0])
    row["tips"] = outcomes[0].get("tips")
    yield row


def read_live(p, args):
    """Read landing requests + outcomes for [start_date, end_date) and join them on unique_key."""
    def read(name: str, folder: str, last_day: date):
        return (p
                | f"{name}Paths" >> beam.Create(
                      landing_paths(args.out_lake, folder, args.start_date, last_day))
                | f"Read{name}"  >> beam.io.ReadAllFromText()      # days with no files are skipped
                | f"Parse{name}" >> beam.Map(json.loads)
                | f"Key{name}"   >> beam.Map(lambda r: (r.get("unique_key"), r)))

    requests = read("Requests", "live_trips", args.end_date)
    outcomes = read("Outcomes", "ground_truth", args.end_date + timedelta(days=OUTCOME_GRACE_DAYS))

    return ({"request": requests, "outcome": outcomes}
            | "JoinOnTrip" >> beam.CoGroupByKey()
            | "MergeTrip"  >> beam.FlatMap(merge_trip)
            | "ToRawShape" >> beam.Map(live_to_raw))


# ----------------------------- REGISTRY --------------------------------------

# source name → (reader function, silver dataset folder)
SOURCES = {
    "historical": (read_historical, "trips"),
    "live":       (read_live,       "live_trips"),
}