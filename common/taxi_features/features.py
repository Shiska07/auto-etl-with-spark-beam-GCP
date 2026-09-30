"""
Row-level feature and label logic, shared by feature engineering (Spark), training and serving.

Rules:
- Only use values known at payment time (end of trip).
- Never use tips or trip_total as features (they contain the answer).
- Vocabularies/encodings belong in training since data at this stange benefits from being model agnostic.
  Vocabularies (e.g. "top 20 companies")
   and encodings (one-hot, scaling) are computed from the TRAINING split and
   saved with the model, for two reasons:
   - Learned from data: if they were computed here, over all rows, the val and
     test splits would influence them, which leaks information into evaluation.
   - Model-specific: a tree model needs no scaling and a linear model does.
     Keeping them out of here lets gold stay model-agnostic, so one gold
     version can train any model.
   This module holds only fixed rules that never change between training runs.


Output features
---------------
Passed through (cleaned, nulls filled):
    trip_miles, fare, extras, tolls, pickup_area, dropoff_area, company
Engineered (derived from other columns):
    hour, day_of_week, is_weekend       <- trip_start_ts
    trip_minutes                        <- trip_seconds
    avg_speed_mph                       <- trip_miles, trip_seconds
    fare_per_mile                       <- fare, trip_miles
    has_extras                          <- extras
    is_airport, is_downtown, same_area  <- pickup/dropoff community area

"""

import pandas as pd

TIP_THRESHOLD = 0.20

AIRPORT_AREAS = {56, 76}          # Midway (Garfield Ridge), O'Hare
DOWNTOWN_AREAS = {8, 32, 33}      # Near North Side, Loop, Near South Side
UNKNOWN_AREA = -1              # outside Chicago / missing

FEATURE_COLUMNS = [
    "hour", "day_of_week", "is_weekend",
    "trip_miles", "trip_minutes", "avg_speed_mph",
    "fare", "fare_per_mile", "extras", "tolls", "has_extras",
    "pickup_area", "dropoff_area", "is_airport", "is_downtown", "same_area",
    "company",
]

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with exactly FEATURE_COLUMNS, one row per input row."""
    out = pd.DataFrame(index=df.index)

    # ── Time ──
    ts = pd.to_datetime(df["trip_start_ts"])
    out["hour"] = ts.dt.hour                                 # nightlife vs commute vs daytime riders tip differently
    out["day_of_week"] = ts.dt.dayofweek                     # Monday = 0; weekday business vs weekend leisure
    out["is_weekend"] = (out["day_of_week"] >= 5).astype(int)  # explicit flag makes the weekend effect easy to learn

    # ── Trip ──
    miles = df["trip_miles"].astype(float)
    seconds = df["trip_seconds"].astype(float)
    hours = seconds / 3600
    out["trip_miles"] = miles                                # tip % often drops on longer trips
    out["trip_minutes"] = seconds / 60                       # minutes are easier to read than seconds
    out["avg_speed_mph"] = (miles / hours).where(hours > 0, 0.0)  # stuck in traffic vs smooth ride → satisfaction

    fare = df["fare"].astype(float)
    out["fare"] = fare                                       # riders tip less generously on expensive fares
    out["fare_per_mile"] = (fare / miles).where(miles > 0, 0.0)   # how pricey the trip felt (traffic, short hops)
    out["extras"] = df["extras"].fillna(0).astype(float)     # extra charges can reduce willingness to tip
    out["tolls"] = df["tolls"].fillna(0).astype(float)       # same idea as extras; missing means none
    out["has_extras"] = (out["extras"] > 0).astype(int)      # whether any extra was charged may matter more than the amount

    # ── Place ──
    pickup = df["pickup_community_area"].fillna(UNKNOWN_AREA).astype(int)
    dropoff = df["dropoff_community_area"].fillna(UNKNOWN_AREA).astype(int)
    out["pickup_area"] = pickup                              # neighbourhood effects (downtown, nightlife, residential)
    out["dropoff_area"] = dropoff
    out["is_airport"] = (pickup.isin(AIRPORT_AREAS) | dropoff.isin(AIRPORT_AREAS)).astype(int)     # airport riders (travellers, expense accounts) behave differently
    out["is_downtown"] = (pickup.isin(DOWNTOWN_AREAS) | dropoff.isin(DOWNTOWN_AREAS)).astype(int)  # tourists and business travellers
    out["same_area"] = ((pickup == dropoff) & (pickup != UNKNOWN_AREA)).astype(int)                # short local hop vs cross-city trip

    # ── Operator ──
    # Companies use different payment terminals with different preset tip
    # buttons (e.g. 15/20/25%), likely a strong signal. Kept as the raw name:
    # grouping rare companies into "other" is learned from data (rule 3).
    out["company"] = df["company"].fillna("unknown").str.strip()

    return out[FEATURE_COLUMNS]


def create_label(df: pd.DataFrame) -> pd.Series:
    """1 if the tip is more than 20% of the fare, else 0.

    Shared so training and monitoring use exactly the same definition.
    """
    return ((df["tips"] / df["fare"]) > TIP_THRESHOLD).astype(int)
