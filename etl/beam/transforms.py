from datetime import datetime
import apache_beam as beam
from apache_beam.metrics import Metrics

# name of second output for bad records
REJECTED = "rejected"

# validation thresholds
MIN_SECONDS, MAX_SECONDS = 60, 4*3600
MIN_MILES, MAX_MILES = 0.1, 100
MIN_FARE, MAX_FARE = 3.25, 500
MIN_AREA, MAX_AREA = 1, 77

def to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def to_int(value):
    f = to_float(value)
    return None if f is None else int(value)


def to_naive_datetime(value):
    """
    BigQuery gives timestamps as timezone-aware datetimes (UTC).
    Tests or CSVs may give strings. Return a datetime with no timezone, or None.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)    # remove timezone info
    try:
        return datetime.fromisoformat(str(value).replace(" UTC",""))
    except ValueError:
        return None



def validate(row):
    """
    Validates row of data for dtype, null values and returns a list of problems.
    """

    # store list of problems for logging
    problems = []

    # try converting datetime
    if to_naive_datetime(row.get("trip_stare_timestamp")) is None:
        problems.append("missing_start_ts")

    seconds = to_float(row.get("trip_seconds"))
    if seconds is None or not (MIN_SECONDS <= seconds <= MAX_SECONDS):
        problems.append("bad_duration")

    miles = to_float(row.get("trip_miles"))
    if miles is None or not (MIN_MILES <= miles <= MAX_MILES):
        problems.append("bad_distance")

    fare = to_float(row.get("fare"))
    if fare is None or not (MIN_FARE <= fare <= MAX_FARE):
        problems.append("bad_fare")

    tips = to_float(row.get("tips"))
    if tips is None or tips < 0:
        problems.append("bad_tips")

    pickup = to_int(row.get("pickup_community_area"))
    if pickup is None or not (MIN_AREA <= pickup <= MAX_AREA):
        problems.append("bad_pickup_area")

    

    return problems

class CleanTrip(beam.DoFn):
    """
    Beam DoFn object for cleaning trip data rows.
    For each trip:
    - not a credit card payment -> dropped (tips aren't recorded for cash)
    - has data-quality problems -> sent to the REJECTED output
    - otherwise                 -> clean, typed record on the main output
    """
    def __init__(self):
        super().__init__()
        self.valid = Metrics.counter("clean_trip", "valid_rows")
        self.rejected = Metrics.counter("clean_trip", "rejected_rows")
        self.filtered = Metrics.counter("clean_trip", "filtered_not_credit_card")

    def process(self, row):
        # Beam calls process() once for every element in the input PCollection.

        # silently drop non-card trips
        if row.get("payment_type") != "Credit Card":
            self.filtered.inc()  # increase counter
            return  # no output for this row

        # Data-quality check: send bad rows to the rejected output
        problems = validate(row)
        if problems:
            self.rejected.inc()  # increase counter
            # yield (not return) emits an element and lets the DoFn keep running.
            # TaggedOutput sends it to a named side output ("rejected") instead of the main one.
            yield beam.pvalue.TaggedOutput(REJECTED, {
                "unique_key": row.get("unique_key"),
                "problems": problems,
                # str() so the raw row can be written as JSON later (datetimes aren't JSON)
                "raw": {k: None if v is None else str(v) for k, v in row.items()},
            })
            return 

        self.valid.inc()  # increase counter for valid rows
        # A plain yield goes to the MAIN output (the "valid" stream)
        yield {
            "unique_key": row["unique_key"],
            "taxi_id": row.get("taxi_id") or "unknown",
            "trip_start_ts": to_naive_datetime(row["trip_start_timestamp"]),
            "trip_seconds": to_int(row["trip_seconds"]),
            "trip_miles": to_float(row["trip_miles"]),
            "fare": to_float(row["fare"]),
            "tips": to_float(row["tips"]),
            "pickup_community_area": to_int(row["pickup_community_area"]),
            "dropoff_community_area": to_int(row.get("dropoff_community_area")),
            "tolls": to_float(row.get("tolls")) or 0.0,
            "extras": to_float(row.get("extras")) or 0.0,
            "company": (row.get("company") or "Unknown").strip(),
        }

