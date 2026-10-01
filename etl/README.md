# ETL Pipelines

Two pipelines turn raw taxi trips into training data:

| Step | Tool | Input | Output |
|---|---|---|---|
| Cleaning | Apache Beam on Dataflow | BigQuery or the GCS landing zone | `silver/` clean Parquet |
| Features | Apache Spark on Dataproc Serverless | One silver run | `gold/` versioned dataset |

```
landing/  raw production batches (JSON)
silver/   cleaned records, one folder per run
rejected/ records that failed validation, with reasons
gold/     features + label, split into train / val / test
```

---

## 1. Data Exploration

Profiling the public table (`bigquery-public-data.chicago_taxi_trips`) with SQL found:

- missing values and duplicate trip IDs
- impossible trips (zero distance, negative fares, extreme durations)
- **tips are only recorded for card payments**

Cash trips are excluded, so the model learns real tipping behavior instead of a recording
gap. Queries are in `sql_queries/`.

---

## 2. Cleaning Pipeline (Beam → silver)

### Sources

The pipeline has one cleaning path and two readers, chosen with `--source`:

| Source | Reads from | Used for |
|---|---|---|
| `historical` | BigQuery, via `sql_queries/02_load_table.sql` | Training data |
| `live` | `landing/live_trips/` + `landing/ground_truth/` (JSON) | Production batches |

For `live`, each trip request is joined with its outcome (tips) on `unique_key`. Outcomes are
read one extra day past the end date, since a trip can finish after midnight. Trips with no
outcome yet are skipped and picked up by a later run.

A date range becomes one file pattern per day
(`landing/live_trips/event_date=YYYY-MM-DD/*.json`), so no extra tables are needed.

### Steps

1. **Read** from the selected source and convert rows to one common shape.
2. **Validate** each record with `CleanTrip`. It has three outcomes:
   - *filtered:* out of scope (e.g. cash payments)
   - *rejected:* fails a rule; written to `rejected/` with the reasons
   - *valid:* typed and passed on
3. **Deduplicate** on `unique_key`.
4. **Write** Parquet to silver, and the rejects as JSON lines.

Beam metrics counters track valid, filtered and rejected rows.

### Output layout

```
silver/<dataset>/run_id=<start>_<end>_<timestamp>/part-*.parquet
rejected/<dataset>/run_id=<start>_<end>_<timestamp>/part-*.jsonl
```

- `<dataset>` is `trips` (historical) or `live_trips` (live).
- Each run gets a new folder, so reruns never overwrite or duplicate data.
- Downstream jobs read one explicit run folder.
- `--run_ts` can be passed in so an orchestrator knows the output path before the job starts.

### Files

| File | Purpose |
|---|---|
| `beam_etl/transforms.py` | Validation rules and the `CleanTrip` transform |
| `beam_etl/sources.py` | Readers for each source(live batched/historical training), and the `SOURCES` registry |
| `beam_etl/pipeline.py` | Arguments, output paths, read → clean → dedupe → write |
| `setup.py` | Packages `beam_etl` for Dataflow workers |

---

## 3. Flex Template

The cleaning pipeline is packaged as a **Dataflow Flex Template**, so it can be launched with
parameters only. No local Python environment is needed.

- **Image** (Artifact Registry): code, SQL and pinned dependencies (Python 3.12, Beam 2.76.0).
- **Spec file** (GCS): points to the image and lists the allowed parameters.

| File | Purpose |
|---|---|
| `beam_etl/Dockerfile` | Builds the launcher image |
| `beam_etl/cloudbuild.yaml` | Cloud Build steps for the image |
| `beam_etl/metadata.json` | Template parameters and validation rules |
| `.gcloudignore` | Keeps Spark jobs and tests out of the build |

Each build is tagged with the git commit:

```
<AR_REPO>/clean-trips:<commit>                            image
gs://<code-bucket>/templates/clean-trips/<commit>.json    spec
gs://<code-bucket>/templates/clean-trips/latest.json      current release
```

Older commits stay available for comparison or rollback.

### Parameters

| Parameter | Required | Example |
|---|---|---|
| `source` | No (default `historical`) | `live` |
| `start_date` | Yes | `2022-04-01` |
| `end_date` | Yes (exclusive) | `2022-04-08` |
| `out_lake` | Yes | `gs://<lake-bucket>` |
| `run_ts` | No | `20260101-120000` |

---

## 4. Feature Pipeline (Spark → gold)

### Prediction moment

The model predicts **at the end of the trip, at payment**. A feature is allowed only if it is
known at that moment and does not include the tip.

### Label

`label = 1` if `tips / fare > 0.20`, else `0`. Defined once in `common/`.

### Features (v1)

All features are computed from a single trip, so serving needs no extra lookups.

| Group | Features | Reason |
|---|---|---|
| Time | `hour`, `day_of_week`, `is_weekend` | Commuter, nightlife and weekend riders tip differently |
| Trip | `trip_miles`, `trip_minutes`, `avg_speed_mph` | Long or slow trips can change tipping |
| Cost | `fare`, `fare_per_mile`, `extras`, `tolls`, `has_extras` | Expensive or surcharged trips can lower tip % |
| Place | `pickup_area`, `dropoff_area`, `is_airport`, `is_downtown`, `same_area` | Location effects, e.g. airport travellers |
| Operator | `company` | Companies use different payment terminals and tip presets |

Encodings learned from data (e.g. category vocabularies) are **not** stored in gold. They are
fitted at training time on the train split only, so gold stays model-agnostic.

### How it works

- Spark reads one silver run and runs the shared pandas code from `common/` with
  `mapInPandas`. Serving uses the same functions.
- **Time-based split (~80/10/10):** cut dates are computed from the data, so validation and
  test data always come after training data.
- **Output:** `gold/taxi_tips/<version>/split=train|val|test/`. Writing to an existing version
  fails instead of overwriting.
- **Manifest (`_manifest.json`):** source silver run, git commit, label rule, feature list,
  split dates, and row count and label rate per split. It is written last, so a version with
  no manifest is incomplete.

| File | Purpose |
|---|---|
| `spark_jobs/build_gold_dataset.py` | Spark job: silver → gold + manifest |
| `../common/taxi_features/` | Shared feature and label code |

### Planned (v2)

Aggregate features (e.g. historical tip rate per area or company). These need point-in-time
correctness and a lookup at serving time.

---

## 5. Validation and Tests

- **SQL checks** on each silver run (duplicates, nulls, date range), using a BigQuery external
  table over the Parquet files. Queries are in `sql_queries/`.
- **Unit tests** (pytest, Beam `TestPipeline`) for validation rules, readers and features.
  They run without GCP.

```bash
pytest etl common -v   # from the repo root
```

---

## 6. How to Run

All commands run from the repo root:

```bash
source config/dev.env
```

| Script | What it does |
|---|---|
| `simulation/00_seed_landing.sh [START END]` | Writes held-out data to the landing zone as daily batches |
| `etl/scripts/00_run_beam_pipeline.sh START END` | Runs the Beam pipeline on Dataflow from Cloud Shell |
| `etl/scripts/01_build_beam_template.sh` | Builds the Flex Template image and spec |
| `etl/scripts/02_run_spark_feature_job.sh SILVER_PATH VERSION` | Runs the Spark job on Dataproc Serverless |

**Launch the template:**

```bash
gcloud dataflow flex-template run "clean-trips-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location "$CODE/templates/clean-trips/latest.json" \
  --region "$REGION" \
  --service-account-email "$SA" \
  --staging-location "$TEMP/dataflow/staging" \
  --temp-location "$TEMP/dataflow/temp" \
  --disable-public-ips \
  --max-workers 2 \
  --parameters source=live,start_date=2022-04-01,end_date=2022-04-08,out_lake=$LAKE
```

**Build a gold dataset:**

```bash
bash etl/scripts/02_run_spark_feature_job.sh "$LAKE/silver/trips/run_id=<run_id>" v1
```

---
