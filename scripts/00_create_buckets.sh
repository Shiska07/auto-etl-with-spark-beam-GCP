#!/usr/bin/env bash
# =============================================================================
# 00_create_buckets.sh
# One-time setup: enables the required GCP APIs and creates the three buckets
# used by the Chicago taxi ETL project.
#
#   LAKE : the data itself (silver/, gold/, rejects/). Versioned, never auto-deleted.
#   CODE : deployed job code (Spark scripts, Beam templates).
#   TEMP : staging/scratch files for Dataflow and Dataproc. Auto-deleted after 7 days.
#
# Safe to re-run: buckets that already exist are skipped.
# Usage: bash scripts/00_create_buckets.sh
# =============================================================================

# Stop on the first error (-e), on undefined variables (-u), and on a failure
# anywhere in a pipe (-o pipefail). Prevents half-finished setups.
set -euo pipefail

# Load PROJECT_ID, REGION, LAKE, CODE, TEMP from the shared config file.
# The path is relative to this script, so it works from any folder.
source "$(dirname "$0")/../config/dev.env"

# Point gcloud at the right project, in case Cloud Shell is set to another one.
gcloud config set project "$PROJECT_ID"

echo ">>> Enabling APIs (can take a minute or two)..."
gcloud services enable \
  dataflow.googleapis.com \
  dataproc.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com \
  storage.googleapis.com \
  compute.googleapis.com \
  notebooks.googleapis.com

echo ">>> Creating buckets in $REGION..."
for BUCKET in "$LAKE" "$CODE" "$TEMP"; do
  # 'describe' succeeds only if the bucket already exists -> skip it.
  if gcloud storage buckets describe "$BUCKET" >/dev/null 2>&1; then
    echo "    exists, skipping: $BUCKET"
  else
    # Uniform bucket-level access = permissions are managed with IAM only
    # (no per-object ACLs). This is Google's recommended default.
    gcloud storage buckets create "$BUCKET" \
      --location="$REGION" \
