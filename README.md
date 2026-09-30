# Chicago Taxi Tips: An End-to-End ML Platform on Google Cloud

Pruduction level implementation on END-to-END ML System for predicting whether a Chicago taxi rider will leave a generous tip (more than 20%). 
This project focuses on building the broader ML infrastructure rather than focused model development and training. 
The raw data is millions of trip records with missing values, duplicates, impossible trips and a hidden bias in how tips are recorded.

This repository builds the full path from raw data to a production-style ML system on
**Google Cloud Platform**: a distributed **ETL pipeline** with **Apache Beam** and
**Apache Spark**, followed by model training, evaluation and deployment on **Vertex AI**,
with **CI/CD** and post-deployment evaluation.



## ETL Pipeline

The ETL turns ~200M raw trip records in **BigQuery** into a clean, feature-rich,
training-ready dataset in **Cloud Storage**. Each tool is used for the job it's best at:


BigQuery public dataset (raw trips)
        │
        │  Apache Beam on Dataflow
        │  row-level validation, cleaning, deduplication, dead-letter handling
        ▼
GCS lake: silver/   (clean, typed Parquet)
        │
        │  Apache Spark on Dataproc Serverless
        │  joins, window functions, feature engineering, time-based splits
        ▼
GCS lake: gold/     (training-ready dataset → Vertex AI)

```

**Beam for ingestion and cleaning** Cleaning is record-by-record work: validate a
trip, fix its types, route bad records aside. Beam's model handles this naturally, scales
automatically on Dataflow with no cluster to manage, and the same code could later process
a live **Pub/Sub** stream instead of a batch table.

**Spark for feature engineerin?** Feature engineering requires looking across many
rows at once. Spark's DataFrame API, **window functions** and joins handle this at scale,
and **Dataproc Serverless** runs it without provisioning a cluster.

### Key finding from data exploration

Exploratory analysis in **BigQuery SQL** showed that tips are only recorded for credit
card payments. Cash trips almost always show a $0 tip because they are not entered. 
Keeping them would teach a model that "cash means no
tip," which is a data artifact, not behavior. The pipeline therefore keeps credit card
trips only.


## GCP Foundation

All infrastructure is provisioned with version-controlled, re-runnable **Bash** scripts
using the **gcloud CLI**, so the environment can be rebuilt from scratch in minutes.

- **Storage architecture:** three purpose-built **Cloud Storage** buckets. A *lake* bucket
  for pipeline data (object versioning enabled to protect against accidental overwrites),
  a *code* bucket for deployed job artifacts, and a *temp* bucket for staging files with a
  7-day **lifecycle rule** for automatic cleanup.
- **Least-privilege security:** a dedicated **service account** for pipeline workers, with
  specific **IAM roles** it needs (Dataflow worker, Dataproc worker, BigQuery job user)
  and storage access scoped to the project's buckets rather than project-wide.
- **Networking:** **Private Google Access** enabled so Dataproc Serverless workers, which
  have no public IPs, can still reach BigQuery and Cloud Storage securely.
- **Cost controls:** a project budget with email alerts, and queries that select only
  needed columns to minimize BigQuery scan costs.


## Repository Structure

.
├── config/
│   └── dev.env                  # Project ID, region, bucket names, service account
├── infra/
│   └── scripts/
│       ├── 00_create_buckets.sh     # Enables APIs, creates buckets, versioning, lifecycle rules
│       └── 01_service_account.sh    # Service account, IAM roles, networking
├── etl/
│   ├── sql/
│   │   └── 01_explore.sql       # Exploratory analysis: data quality, payment types, tip distribution
│   ├── beam/                    # Apache Beam pipelines
│   └── requirements.txt         # ETL dependencies
└── README.md
```

Upcoming components (`training/`, `evaluation/`, `serving/`, `pipelines/`, CI/CD) will
each live in their own folder with their own README and dependencies.



## Tech Stack

**Data & processing:** BigQuery · SQL · Apache Beam · Google Cloud Dataflow · Apache Spark (PySpark) · Dataproc Serverless · Parquet
**Infrastructure:** Google Cloud Storage · IAM · VPC networking · gcloud CLI · Bash
**Development:** Python · Git · GitHub · Cloud Shell
**Planned:** Vertex AI · Cloud Build / GitHub Actions · Cloud Composer (Airflow)


## Getting Started

```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
source config/dev.env

# One-time infrastructure setup (safe to re-run)
bash infra/scripts/00_create_buckets.sh
bash infra/scripts/01_service_account.sh

# ETL environment
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```
