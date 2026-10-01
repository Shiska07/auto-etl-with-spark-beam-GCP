"""
pipeline.py
Beam ETL: raw trips → clean, remove duplicates → Parquet in the GCS lake (silver layer).

  read (sources.py: historical | live) → CleanTrip (valid / rejected) → dedupe
  → silver Parquet + rejects JSON

Run from the etl/ folder:
  python -m beam_etl.pipeline --source historical --start_date 2022-01-01 --end_date 2022-01-02 \
      --out_lake $LAKE --project $PROJECT_ID --temp_location $TEMP/beam-local
  python -m beam_etl.pipeline --source live --start_date 2022-04-01 --end_date 2022-04-08 \
      --out_lake $LAKE --project $PROJECT_ID --temp_location $TEMP/beam-local
"""

import argparse
import json
import logging
from datetime import date, datetime, timezone

import apache_beam as beam
import pyarrow as pa
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions

from beam_etl.sources import SOURCES
from beam_etl.transforms import REJECTED, CleanTrip

# Schema of the cleaned Parquet files; must match what CleanTrip emits
SILVER_SCHEMA = pa.schema([
    ("unique_key", pa.string()),
    ("taxi_id", pa.string()),
    ("trip_start_ts", pa.timestamp("us")),
    ("trip_seconds", pa.int64()),
    ("trip_miles", pa.float64()),
    ("fare", pa.float64()),
    ("tips", pa.float64()),
    ("pickup_community_area", pa.int64()),
    ("dropoff_community_area", pa.int64()),   # nullable (trips ending outside the city)
    ("tolls", pa.float64()),
    ("extras", pa.float64()),
    ("company", pa.string()),
])


def valid_date(s: str) -> date:
    try:
        return date.fromisoformat(s)          # "2022-01-01" → date(2022, 1, 1)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{s}' is not a valid date (expected YYYY-MM-DD)")


def keep_first(key_and_rows):
    """After grouping by unique_key (key, Iterable[row]), keep a single row per trip."""
    _key, rows = key_and_rows
    return next(iter(rows))


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=SOURCES.keys(), default="historical",
                        help="historical = BigQuery public table, live = landing files")
    parser.add_argument("--start_date", type=valid_date, required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--end_date", type=valid_date, required=True, help="YYYY-MM-DD, exclusive")
    parser.add_argument("--out_lake", required=True, help="Lake bucket, e.g. gs://<project>-lake")
    parser.add_argument("--run_ts", default=None,
                        help="UTC run timestamp YYYYMMDD-HHMMSS; generated if not given")

    # parse_known_args: our args go to `args`; everything else (--project, --runner, ...) goes to Beam
    args, beam_args = parser.parse_known_args(argv)

    if args.start_date >= args.end_date:
        parser.error("--start_date must be before --end_date")

    reader, dataset = SOURCES[args.source]

    # output paths: one immutable folder per run
    run_ts = args.run_ts or datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_id = f"{args.start_date}_{args.end_date}_{run_ts}"
    silver_dir = f"{args.out_lake}/silver/{dataset}/run_id={run_id}"
    rejects_dir = f"{args.out_lake}/rejected/{dataset}/run_id={run_id}"
    print(f">> Source:  {args.source}")
    print(f">> Silver:  {silver_dir}")
    print(f">> Rejects: {rejects_dir}")

    options = PipelineOptions(beam_args)

    # job name (Dataflow needs a unique one; lowercase letters, digits and hyphens only)
    gcp = options.view_as(GoogleCloudOptions)
    if not gcp.job_name:
        gcp.job_name = f"taxi-clean-{args.source}-{run_id.replace('_', '-')}"

    with beam.Pipeline(options=options) as p:

        # read: the only source-specific step
        raw = reader(p, args)

        # validate and clean
        cleaned = raw | "CleanTrip" >> beam.ParDo(CleanTrip()).with_outputs(REJECTED, main="valid")

        # remove duplicates
        deduped = (
            cleaned.valid                                                  # rows as dicts
            | "KeyByTripID" >> beam.Map(lambda r: (r["unique_key"], r))    # (unique_key, row)
            | "GroupByKey"  >> beam.GroupByKey()                           # (unique_key, Iterable[row])
            | "KeepFirst"   >> beam.Map(keep_first)                        # one row per trip
        )

        # write clean data
        deduped | "WriteSilver" >> beam.io.WriteToParquet(
            file_path_prefix=f"{silver_dir}/part",
            schema=SILVER_SCHEMA,
            file_name_suffix=".parquet",
        )

        # write rejected data as json lines
        (cleaned[REJECTED]
         | "RejectsToJson" >> beam.Map(json.dumps, default=str)
         | "WriteRejects"  >> beam.io.WriteToText(
               file_path_prefix=f"{rejects_dir}/part",
               file_name_suffix=".jsonl",
           ))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()