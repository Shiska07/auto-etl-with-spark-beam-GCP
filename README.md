# End-to-End ML System on Google Cloud

A production-style machine learning system built on **Google Cloud Platform**, covering the
full lifecycle: distributed ETL, model training, validation, deployment, and post-deployment
monitoring against live (simulated) traffic.

The use case, predicting whether a Chicago taxi rider tips more than 20%. The focus is the **infrastructure and MLOps workflow**.

## Tech Stack

**Data:** BigQuery · SQL · Apache Beam · Dataflow · Apache Spark · Dataproc Serverless · Parquet
**ML:** Vertex AI Training · Model Registry · Endpoints · Model Monitoring · Pipelines (planned)
**Infrastructure:** Cloud Storage · IAM · Pub/Sub · gcloud CLI · Bash (planned)
**CI/CD Automation:** Python · pytest · Git · GitHub Actions (planned)


## System Overview

```
BigQuery (raw data)
   │  Apache Beam on Dataflow        → validation, cleaning, deduplication
   ▼
GCS silver/ (clean Parquet)
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
- **Versioned, immutable datasets:** each gold dataset (`gold/v1`, `gold/v2`) is written once,
  and every registered model records which version trained it.
- **Held-out time window for real-world simulation:** a later period of data is excluded from training
  and replayed through Pub/Sub, so live predictions can be scored against known ground truth.
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

**Cleaning & validation (Apache Beam), in progress.**
- Business rules are kept separate from pipeline I/O, so they can be unit tested without GCP.
- A `DoFn` routes each record to one of three outcomes: filtered, rejected to a
  **dead-letter output** with the failure reasons, or emitted as a clean, typed record.
- **Beam metrics counters** track valid, rejected and filtered rows.
- Tested with **pytest** and Beam's `TestPipeline`.

---

## Repository Structure

```
.
├── config/dev.env            # project, region, buckets, service account
├── infra/scripts/            # bucket, IAM and networking setup
└── etl/
    ├── sql/                  # BigQuery exploration queries
    ├── beam/                 # Beam pipelines and transforms
    ├── tests/                # unit and pipeline tests
    └── requirements.txt
```

Each future component (`training/`, `serving/`, `simulation/`, `monitoring/`, `pipelines/`)
will live in its own folder with its own dependencies and tests.


## Getting Started

```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
source config/dev.env

bash infra/scripts/00_create_buckets.sh
bash infra/scripts/01_service_account.sh

python3 -m venv .venv && source .venv/bin/activate
pip install -r etl/requirements.txt
pytest etl -v
```