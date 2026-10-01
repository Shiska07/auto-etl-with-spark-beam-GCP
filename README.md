# End-to-End ML System on Google Cloud

A production-style machine learning system on **Google Cloud Platform**. It covers the full
lifecycle: data pipelines, model training, deployment, and monitoring against production traffic.

The use case is predicting whether a Chicago taxi rider tips more than 20%. The focus is the
**infrastructure and MLOps workflow for Automation**.

## Tech Stack

- **Data:** BigQuery · SQL · Apache Beam · Dataflow · Apache Spark · Dataproc Serverless · Parquet
- **ML:** Vertex AI Training · Model Registry · Endpoints · Model Monitoring · Pipelines
- **Infrastructure:** Cloud Storage · IAM · gcloud CLI · Bash · Pub/Sub · Docker · Cloud Build · Artifact Registry
- **CI/CD:** Python · pytest · Git · GitHub Actions

## Status

| Part | Status |
|---|---|
| GCP foundation (buckets, IAM, networking) 
| Cleaning pipeline (Apache Beam → silver) 
| Feature pipeline (Spark → gold) 
| Cleaning pipeline as a Dataflow Flex Template | In progress |
| Pipeline automation (Workflows + Scheduler) | Planned |
| Training, registry, serving, monitoring | Planned |

## Architecture

```
BigQuery (historical trips)     GCS landing/ (production batches)
          └──────────────┬──────────────┘
                         │  Apache Beam on Dataflow   → validation, cleaning, deduplication
                         ▼
GCS silver/   clean Parquet, one immutable folder per run
                         │  Apache Spark on Dataproc  → features, label, time-based split
                         ▼
GCS gold/     versioned training datasets + manifest
                         │  Vertex AI Training        → train and evaluate
                         ▼
Vertex AI Model Registry  versioned models with data lineage
                         │
                         ▼
Vertex AI Endpoint  ◄── Pub/Sub ◄── held-out data replayed as production traffic
                         │
                         ▼
BigQuery prediction log + ground truth → live evaluation, drift monitoring, retraining
```

## Key Design Choices

- **Beam for cleaning, Spark for features.** Beam handles record-level checks and can move
  from batch to streaming with the same code. Spark handles dataset-wide work such as splits
  and aggregations.
- **One cleaning pipeline for training and production data.** Historical and live batches use
  the same validation code. 
- **Immutable, versioned data.** Every run writes to its own folder and is never overwritten.
  Manifest files link each dataset to its source data and code version.
- **Shared feature code.** Feature and label logic lives in one package (`common/`), used by
  training, serving and monitoring, to avoid training/serving skew.
- **Realistic evaluation.** A later time window is held out from training and replayed as
  production traffic, so live predictions can be scored against known outcomes.
- **Serverless where possible.** Dataflow, Dataproc Serverless and Vertex AI, so there are
  no clusters to manage.

## Repository Structure

```
.
├── config/dev.env      # project, region, buckets, service account, date windows
├── infra/              # one-time setup: buckets, IAM, networking, Artifact Registry
├── common/             # shared feature and label logic
├── simulation/         # writes held-out data into the landing zone as production batches
└── etl/                # data pipelines: Beam (silver) and Spark (gold)
```

## Documentation

- [ETL pipelines](etl/README.md): sources, cleaning, features, versioning, how to run

## Quick Start

```bash
git clone https://github.com/Shiska07/gcp-mlops-tabular.git
cd gcp-mlops-tabular
source config/dev.env

# One-time infrastructure setup
bash infra/00_create_buckets.sh
bash infra/01_service_account.sh
bash infra/02_artifact_registry.sh

# Python environment and tests
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -e common/
pytest common etl -v
```

See [etl/README.md](etl/README.md) to run the pipelines.