"""
pipeline.py
Beam ETL: BigQuery raw trips → clean, remove duplicates and save Parquet in the GCS lake (silver layer).

  read (BigQuery) → CleanTrip (valid / rejected) → dedupe → write Parquet + rejects JSON

Run from the etl/ folder:
  python -m beam.pipeline --start_date 2022-01-01 --end_date 2022-01-02 \
      --lake $LAKE --project $PROJECT_ID --temp_location $TEMP/beam-local
"""

import json
import argparse
import datetime
import logging
from pathlib import Path
from datetime import datetime, timezone
from datetime import date

import pyarrow as pa
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

from beam_etl.transforms import CleanTrip, REJECTED


SQL_DIR = Path(__file__).resolve().parent.parent / "sql_queries" 

# Schema or cleaned parquet file, must match schema returned by transforms.py CleanTrip(beam.Dofn)
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

# helpers
def valid_date(s: str) -> date:
    try:
        return date.fromisoformat(s)          # "2022-01-01" → date(2022, 1, 1)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{s}' is not a valid date (expected YYYY-MM-DD)")


def keep_first(key_and_rows):
    """After grouping by unique_key (key, Iterable[raw]), keep a single row per trip."""
    _key, rows = key_and_rows
    return next(iter(rows))


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", type=valid_date, required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--end_date", type=valid_date, required=True, help="YYYY-MM-DD, exclusive")
    parser.add_argument("--out_lake", required=True, help="Lake bucket, e.g. gs://<project>-lake")
    parser.add_argument("--query_file", default="02_load_table.sql",
                    help="SQL file name inside sql_queries/")

    # parse_known_args: our args go to `args`; everything else (--project, --runner, ...) goes to Beam
    args, beam_args = parser.parse_known_args(argv)


    # load query
    if args.start_date >= args.end_date:
        parser.error("--start_date must be before --end_date")
    
    query = (SQL_DIR / args.query_file).read_text().format(start_date=args.start_date, end_date=args.end_date)
    load_date = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    # bucket paths
    run_id = run_id = f"{args.start_date}_{args.end_date}_{load_date}"
    silver_dir = f"{args.out_lake}/silver/trips/run_id={run_id}"
    rejects_dir = f"{args.out_lake}/rejected/trips/run_id={run_id}"

    options=PipelineOptions(beam_args)

    # add job name here (this is required for dataflow service)
    gcp = options.view_as(GoogleCloudOptions)
    if not gcp.job_name:
        gcp.job_name = f"taxi-clean-{run_id.replace('_', '-')}"

    with beam.Pipeline(options=options) as p:

        # load raw data
        raw = p | "ReadBigQuery" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)

        # validate and clean
        cleaned = raw | "CleanTrip" >> beam.ParDo(CleanTrip()).with_outputs(REJECTED, main="valid")

        # remove duplicates
        deduped = (
            cleaned.valid                                                     # PCollection of rows as dicts
            | "KeyByTripID" >> beam.Map(lambda r: (r["unique_key"], r))     # PCollection of tuples (unique_key, row dict)
            | "GroupByKey"  >> beam.GroupByKey()                            # PCollection of tuples (unique_key, Iterable[rows with the same unique_key])
            | "KeepFirst"  >> beam.Map(keep_first)                          # PCollection of rows as dicts: the first item from Iterable[rows with the same unique_key] 
        )

        # write clean data
        deduped | "WriteSilver" >> beam.io.WriteToParquet(
            file_path_prefix=f"{silver_dir}/part",
            schema=SILVER_SCHEMA,
            file_name_suffix='.parquet'
        )

        # write rejected data as json lines
        {
            cleaned[REJECTED]
            | "RejectsToJson" >> beam.Map(json.dumps)
            | "WriteRejects" >> beam.io.WriteToText(
                file_path_prefix=f"{rejects_dir}/part",
                file_name_suffix='.jsonl'
            )
        }


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

    