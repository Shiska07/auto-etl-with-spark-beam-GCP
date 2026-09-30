"""
Build gold/<version> from one silver run.

silver run  →  window filter → time split → features + label (common/) → gold/<version>/split=*/
                                                                        → gold/<version>/_manifest.json
"""

import argparse
import json
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import SparkSession, functions as f

from taxi_features.features import FEATURE_COLUMNS, build_features, create_label, TIP_THRESHOLD

# Output schema for mapInPandas: one (name, Spark type) per column.
GOLD_COLUMNS = [
    # keys
    ("unique_key",     "string"),
    ("trip_start_ts",  "timestamp"),
    ("split",          "string"),
    # time features
    ("hour",           "bigint"),
    ("day_of_week",    "bigint"),
    ("is_weekend",     "bigint"),
    # trip features
    ("trip_miles",     "double"),
    ("trip_minutes",   "double"),
    ("avg_speed_mph",  "double"),
    ("fare",           "double"),
    ("fare_per_mile",  "double"),
    ("extras",         "double"),
    ("tolls",          "double"),
    ("has_extras",     "bigint"),
    # place features
    ("pickup_area",    "bigint"),
    ("dropoff_area",   "bigint"),
    ("is_airport",     "bigint"),
    ("is_downtown",    "bigint"),
    ("same_area",      "bigint"),
    # operator feature
    ("company",        "string"),
    # target
    ("label",          "bigint"),
]
GOLD_SCHEMA = ", ".join(f"{name} {dtype}" for name, dtype in GOLD_COLUMNS)
