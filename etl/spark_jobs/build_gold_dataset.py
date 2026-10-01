"""
Build gold/<version> from one silver run.
References common.taxi_features.features for feature engineering workflow
silver run  →  window filter → time split → features + label (common/) → gold/<version>/split=*/
                                                                        → gold/<version>/_manifest.json
"""

from gc import collect
from pandas import DataFrame
from collections.abc import Iterator
import argparse
import json
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import SparkSession, functions as F

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


# ----------------------------- HELPERS ---------------------------------------

# helper to split data into train/val/test based on dates
def compute_split_dates(silver, val_frac, test_frac):
    """
    Return (val_start, test_start) as dates, so that roughly val_frac / test_frac
    of the rows (by trip time) fall into val / test. Cuts are rounded to midnight.
    """

    secs = F.col("trip_start_ts").cast("long")          # timestamp → seconds since 1970 
    q_val, q_test = (silver.select(secs.alias("s"))
                     .approxQuantile("s", [1 - val_frac - test_frac, 1 - test_frac], 0.001))
    to_date = lambda s: datetime.fromtimestamp(s, tz=timezone.utc).date()
    return to_date(q_val), to_date(q_test)


# features.build_features does not include these columns in the output df, so they need to be added later
KEY_COLUMNS: list[str] = ["unique_key", "trip_start_ts", "split"]

# mapping function for spark mapToPandas
def to_gold(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """
    For each batch of dataframes, get the ourput dataframes with additional engineered features.
    """
    for pdf in batches:
        features = build_features(pdf)
        out = pd.concat([pdf[KEY_COLUMNS], features], axis=1)
        out["label"]= create_label(pdf)
        yield out


def write_text(spark: SparkSession, path: str, text: str) -> None:
    """Write one small text file (e.g. _manifest.json) to GCS or local disk.

    Uses Spark's Hadoop file system, so the same code works for 'gs://…' paths
    on Dataproc and for local paths when testing. Fails if the file already exists.
    """
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.create(jvm_path, False)       # False = do not overwrite
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()   


def main() -> None:
    p = argparse.ArgumentParser(description="Build a gold dataset version from one silver run.")

    # Input / output
    p.add_argument("--silver_path", required=True,
                   help="One silver run folder (its date range is the dataset window)")
    p.add_argument("--gold_root", required=True,
                   help="Parent folder for gold versions, e.g. gs://…-lake/gold/taxi_tips")
    p.add_argument("--version", required=True,
                   help="Gold version name, e.g. v1 (written once, never overwritten)")
        # Lineage
    p.add_argument("--git_commit", default="unknown",
                   help="Code version that built this dataset (recorded in the manifest)")

    # Split sizes: oldest rows → train, then val, newest → test
    p.add_argument("--val_frac",  type=float, default=0.10)
    p.add_argument("--test_frac", type=float, default=0.10)
    args = p.parse_args()

    # make sure test and val fractions are valid
    if not 0 < args.val_frac + args.test_frac < 1:
        raise ValueError("val_frac + test_frac must be between 0 and 1")

    # ------------------------- START SPARK -----------------------------
    # appName is the name you'll see in the Spark UI and logs.
    spark = SparkSession.builder.appName(f"build-gold-{args.version}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # read silver parquet file
    silver: DataFrame = spark.read.parquet(args.silver_path)

    # Beam wrote timestamps without a time zone (TIMESTAMP_NTZ). Convert to a regular
    # TIMESTAMP; with the session time zone set to UTC above, the values stay unchanged.
    silver = silver.withColumn("trip_start_ts", F.col("trip_start_ts").cast("timestamp"))
    
    # -------------------------- SPLIT DATA -------------------------------
    # find split boundaries from data for validation and testing
    val_start, test_start = compute_split_dates(silver, args.val_frac, args.test_frac)
    print(f">> Split dates: val from {val_start}, test from {test_start}")

    # A reference to the column (no data yet)
    ref_col = F.col("trip_start_ts")

    # The two cut-off moments, as fixed timestamp values
    val_cutoff  = F.lit(str(val_start)).cast("timestamp")    # e.g. 2022-03-08 00:00:00
    test_cutoff = F.lit(str(test_start)).cast("timestamp")   # e.g. 2022-03-18 00:00:00

    # The rule: which split does a trip belong to?
    split_col = (
        F.when(ref_col < val_cutoff, "train")     # before val cutoff  → train
        .when(ref_col < test_cutoff, "val")      # before test cutoff → val
        .otherwise("test")                         # everything later   → test
    )

    # Apply the rule: add a new column called "split" to every row
    silver_with_split = silver.withColumn("split", split_col)

    # -------------------------- FEATURE ENGINEERING -------------------------
    gold = silver_with_split.mapInPandas(to_gold, schema=GOLD_SCHEMA).cache()
    #     └── SPARK in ──────► [ pandas inside to_gold ] ──────► SPARK out ──┘


    # ------------------------- WRITE FINAL GOLD FILE--------------------------
    out_path: str = f"{args.gold_root}/{args.version}"
    (gold.write
        .mode("errorifexists")    # don't overwrite, throw error if exists
        .partitionBy("split")    # split by colun named "split" split=train/ ... split=val/ ... split=test/
        .parquet(out_path))       # save as parquet file

    # ------------------------------ LOGGING ---------------------------------:
    # +-------+---------+----------+---------------------+---------------------+
    # | split |   rows  |label_rate|     first_trip      |      last_trip      |
    # +-------+---------+----------+---------------------+---------------------+
    # | train | 1240512 | 0.4213   | 2022-01-01 00:00:00 | 2022-03-07 23:59:00 |
    # | val   |  155064 | 0.4307   | 2022-03-08 00:00:00 | 2022-03-17 23:59:00 |
    # | test  |  155063 | 0.4255   | 2022-03-18 00:00:00 | 2022-03-31 23:59:00 |
    # +-------+---------+----------+---------------------+---------------------+

    # .collect() gives a python list of row objects instead of a df like list
    # rows = [
    #     Row(split='train', rows=1240512, label_rate=0.42130517, first_trip=datetime(2022, 1, 1, 0, 0), last_trip=datetime(2022, 3, 7, 23, 59)),
    #     Row(split='val',   rows=155064,  label_rate=0.43071893, first_trip=datetime(2022, 3, 8, 0, 0), last_trip=datetime(2022, 3, 17, 23, 59)),
    #     Row(split='test',  rows=155063,  label_rate=0.42551231, first_trip=datetime(2022, 3, 18, 0, 0), last_trip=datetime(2022, 3, 31, 23, 59)),
    # ]
   
    summary_rows = (gold.groupBy("split")
                    .agg(
                        F.count("*").alias("rows"),
                        F.avg("label").alias("label_rate"),
                        F.min("trip_start_ts").alias("first_trip"),
                        F.max("trip_start_ts").alias("last_trip")
                    ).collect())  

    # conver list of rows to dictionary with labels as keys and values as dictionaries. Example:
    # splits = {
    #     "train": {"rows": 1240512, "label_rate": 0.4213, "first_trip": "2022-01-01 00:00:00", "last_trip": "2022-03-07 23:59:00"},
    #     "val":   {"rows": 155064,  "label_rate": 0.4307, "first_trip": "2022-03-08 00:00:00", "last_trip": "2022-03-17 23:59:00"},
    #     "test":  {"rows": 155063,  "label_rate": 0.4255, "first_trip": "2022-03-18 00:00:00", "last_trip": "2022-03-31 23:59:00"},
    # }
    splits = {
        r["split"]: {
            "rows": r["rows"],
            "label_rate": round(r["label_rate"], 4),
            "first_trip": str(r["first_trip"]),
            "last_trip": str(r["last_trip"]),
        }
        for r in summary_rows
    }
    
    #create manifest file using splits
    manifest = {
        "gold_version": args.version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "silver_path": args.silver_path,
        "git_commit": args.git_commit,
        "label": f"tips / fare > {TIP_THRESHOLD}",
        "feature_columns": FEATURE_COLUMNS,
        "split_rule": {"val_frac": args.val_frac, "test_frac": args.test_frac,
                       "val_start": str(val_start), "test_start": str(test_start)},
        "splits": splits,
    }

    # conver to json text
    manifest_test = json.dumps(manifest, indent=2)
    write_text(spark, path=f"{out_path}/_manifest.json", text=manifest_test)
    print(manifest_test)

    spark.stop()

if __name__ == "__main__":
    main()
