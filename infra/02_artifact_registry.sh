#!/usr/bin/env bash
# 02_artifact_registry.sh
# One-time setup for container images: enables APIs, creates the Docker repo,
# and lets the pipeline service account pull images.
set -euo pipefail
source "$(dirname "$0")/../config/dev.env"

REPO=taxi-etl

gcloud services enable artifactregistry.googleapis.com cloudbuild.googleapis.com \
  --project "$PROJECT_ID"

# Docker repository for pipeline images (skip if it already exists)
gcloud artifacts repositories describe "$REPO" --location "$REGION" --project "$PROJECT_ID" >/dev/null 2>&1 \
  || gcloud artifacts repositories create "$REPO" \
       --repository-format docker \
       --location "$REGION" \
       --project "$PROJECT_ID" \
       --description "Pipeline images (Dataflow Flex Templates, later training/serving)"

# The launcher and workers run as $SA and must be able to pull the image
gcloud artifacts repositories add-iam-policy-binding "$REPO" \
  --location "$REGION" --project "$PROJECT_ID" \
  --member "serviceAccount:$SA" \
  --role roles/artifactregistry.reader
  