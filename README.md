# End-to-End ML System on Google Cloud

A production-style machine learning system built on **Google Cloud Platform**, covering the
full lifecycle: distributed ETL, model training, validation, deployment, and post-deployment
monitoring against live (simulated) traffic.

The use case is predicting whether a Chicago taxi rider tips more than 20%. The focus is the
**infrastructure and MLOps workflow**.

## Tech Stack

**Data:** BigQuery · SQL · Apache Beam · Dataflow · Apache Spark · Dataproc Serverless · Parquet
**ML:** Vertex AI Training · Model Registry · Endpoints · Model Monitoring · Pipelines (planned)
**Infrastructure:** Cloud Storage · IAM · gcloud CLI · Bash · Pub/Sub (planned)
**CI/CD Automation:** Python · pytest · Git · GitHub Actions (planned)


## System Overview

```
BigQuery (raw data)
   │  Apache Beam on Dataflow        → validation, cleaning, deduplication
   ▼
GCS silver/ (clean Parquet, one immutable folder per run)
   │  Apache Spark on Dataproc       → feature engineering, time-based splits
   ▼
GCS gold/vN (versioned training data)
   │  Vertex AI Training             → train + validate
   ▼
Vertex AI Model Registry             → versioned models with data lineage
   │
   ▼
Vertex AI Endpoint  ◄── Pub/Sub ◄── held-out data replayed as simulated production traffic
   │
   ▼
BigQuery prediction log + ground truth → live evaluation, drift monitoring, retraining
```

### Key design choices

- **Beam for cleaning, Spark for features:** Beam handles record-level validation and can
  switch from batch to streaming with the same code. Spark handles cross-row work (joins,
  window functions) at scale.
- **Versioned, immutable datasets:** every pipeline run writes to its own folder and is never
  overwritten. Each gold dataset (`gold/v1`, `gold/v2`) records the silver run it was built
  from, and every registered model records which gold version trained it.
- **Held-out time window for real-world simulation:** a later period of data is excluded from
  training and replayed through Pub/Sub, so live predictions can be scored against known
  ground truth.
- **Serverless where possible:** Dataflow, Dataproc Serverless and Vertex AI.

---

## GCP Foundation

Infrastructure is provisioned with re-runnable **Bash** scripts using the **gcloud CLI**.

- **Storage:** three **Cloud Storage** buckets: a versioned data lake, a code bucket for job
  artifacts, and a temp bucket with a 7-day lifecycle rule.
- **Security:** a dedicated **service account** with least-privilege **IAM roles**, and
  bucket access scoped per bucket.
- **Networking:** **Private Google Access** so serverless workers without public IPs can reach
  Google APIs.
- **Cost control:** budget alerts and column-selective BigQuery queries.

## ETL Pipeline

**Data exploration (BigQuery SQL).** Profiling the raw data surfaced missing values,
duplicates, impossible trips, and a recording bias: tips are only captured for card
payments. Cash trips were excluded so the model learns real behavior, not a data artifact.

**Cleaning & validation (Apache Beam on Dataflow).**
- Business rules are kept separate from pipeline I/O, so they can be unit tested without GCP.
- A `DoFn` routes each record to one of three outcomes: filtered, rejected to a
  **dead-letter output** with the failure reasons, or emitted as a clean, typed record.
- Records are **deduplicated** on the trip key before writing.
- **Beam metrics counters** track valid, rejected and filtered rows.
- The same code runs locally or on **Dataflow**, where workers run as the service account
  without public IPs.
- Each run writes to `silver/trips/run_id=<start>_<end>_<timestamp>/`, so reruns never
  overwrite or duplicate data, and downstream jobs read one explicit run.
- Output is checked with **SQL validation queries** (duplicates, nulls, date range) through a
  BigQuery external table over the Parquet files.
- Tested with **pytest** and Beam's `TestPipeline`.

**Feature engineering (Apache Spark on Dataproc Serverless)**, next.

---

## Repository Structure

```
.
├── config/dev.env              # project, region, buckets, service account, date windows
├── infra/                      # bucket, IAM and networking setup scripts
└── etl/
    ├── beam_etl/               # Beam pipeline and transforms (completed)
    ├── spark_jobs/             # Spark feature jobs 
    ├── sql_queries/            # exploration, extraction and validation SQL
    ├── scripts/                # run scripts (local or Dataflow)
    ├── tests/                  # unit and pipeline tests
    ├── setup.py                # packages beam_etl for Dataflow workers
    └── requirements.txt
```

Each future component (`training/`, `serving/`, `simulation/`, `monitoring/`, `pipelines/`)
will live in its own folder with its own dependencies and tests.


## Getting Started

```bash
git clone https://github.com/Shiska07/gcp-mlops-tabular.git
cd gcp-mlops-tabular
source config/dev.env

# One-time infrastructure setup
bash infra/00_create_buckets.sh
bash infra/01_service_account.sh

# Python environment and tests
python3 -m venv .venv && source .venv/bin/activate
pip install -r etl/requirements.txt
pytest etl -v

# Run the cleaning pipeline (local or on Dataflow)
bash etl/scripts/00_run_beam_pipeline.sh 2022-01-01 2022-01-02            # local
bash etl/scripts/00_run_beam_pipeline.sh $TRAIN_START $TRAIN_END dataflow  # Dataflow
```