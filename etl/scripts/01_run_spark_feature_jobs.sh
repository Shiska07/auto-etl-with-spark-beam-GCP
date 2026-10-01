#!/usr/bin/env bash
# 01_run_spark_feature_jobs.sh
# Runs spark feature engineering pipeline on silver data w/ schema post beam processing
# Usage:   bash etl/scripts/01_run_spark_feature_jobs.sh SILVER_PATH VERSION
# Example: bash etl/scripts/01_run_spark_feature_jobs.sh "$LAKE/silver/trips/run_id=<id>" v1
# Optional: VAL_FRAC=0.15 TEST_FRAC=0.15 bash ...   (defaults: 0.10 / 0.10)
set -euo pipefail

JOB_FILE="build_gold_dataset.py"

USAGE="Usage: 00_run_spark_feature_jobs.sh SILVER_PATH VERSION"
SILVER_PATH=${1:?$USAGE}
VERSION=${2:?$USAGE}

# Resolve paths relative to this script, so it works from any folder
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ETL_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$ETL_DIR")"

# prepare to start
source "$REPO_DIR/config/dev.env"

# path to job script
JOB="$ETL_DIR/spark_jobs/$JOB_FILE"

GOLD_ROOT="$LAKE/gold/taxi_tips"
BATCH_ID="gold-${VERSION}-$(date -u +%Y%m%d-%H%M%S)"
GIT_COMMIT="$(git -C "$REPO_DIR" rev-parse --short HEAD)"   # code version, recorded in the manifest

[[ -f "$JOB" ]] || { echo "Job file not found: $JOB" >&2; exit 1; }

# Each serverless batch starts clean, so ship the shared feature code with it
mkdir -p "$REPO_DIR/dist"
rm -f "$REPO_DIR/dist/taxi_features.zip"
(cd "$REPO_DIR/common" && zip -qr "$REPO_DIR/dist/taxi_features.zip" taxi_features)


# Fail early with a clear message if config is missing
: "${LAKE:?LAKE not set in config/dev.env}"
: "${PROJECT_ID:?PROJECT_ID not set in config/dev.env}"
: "${REGION:?REGION not set in config/dev.env}"
: "${TEMP:?TEMP not set in config/dev.env}"
: "${SA:?SA not set in config/dev.env}"


echo ">> Batch: $BATCH_ID"
echo ">> Silver: $SILVER_PATH"
echo ">> Gold:   $GOLD_ROOT/$VERSION"
echo ">> Commit: $GIT_COMMIT"

gcloud dataproc batches submit pyspark "$JOB" \
  --batch "$BATCH_ID" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --service-account "$SA" \
  --version 2.2 \
  --deps-bucket "$TEMP" \
  --py-files "$REPO_DIR/dist/taxi_features.zip" \
  -- \
  --silver_path "$SILVER_PATH" \
  --gold_root "$GOLD_ROOT" \
  --version "$VERSION" \
  --val_frac "${VAL_FRAC:-0.10}" \
  --test_frac "${TEST_FRAC:-0.10}" \
  --git_commit "$GIT_COMMIT"
