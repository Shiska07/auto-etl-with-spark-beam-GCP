#!/usr/bin/env bash
# 00_run_beam_pipeline.sh
# Runs the Beam cleaning pipeline locally or on Dataflow.
# Usage:  bash etl/scripts/00_run_beam_pipeline.sh START_DATE END_DATE [local|dataflow]
# Example: bash etl/scripts/00_run_beam_pipeline.sh 2022-01-01 2022-01-08 dataflow
# Optional: MAX_WORKERS=4 bash etl/scripts/00_run_beam_pipeline.sh 2022-01-01 2022-01-08 dataflow

set -euo pipefail

USAGE="Usage: 00_run_beam_pipeline.sh START_DATE END_DATE [local|dataflow]"
START_DATE=${1:?$USAGE}
END_DATE=${2:?$USAGE}
RUNNER=${3:-local}

# Resolve paths relative to this script, so it works from any folder
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ETL_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$ETL_DIR")"

source "$REPO_DIR/config/dev.env"

# Activate the venv if it isn't already active
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  source "$REPO_DIR/.venv/bin/activate"
fi

# Fail early with a clear message if config is missing
: "${LAKE:?LAKE not set in config/dev.env}"
: "${PROJECT_ID:?PROJECT_ID not set in config/dev.env}"
: "${REGION:?REGION not set in config/dev.env}"
: "${TEMP:?TEMP not set in config/dev.env}"
: "${SA:?SA not set in config/dev.env}"

# Arguments used by both runners
COMMON_ARGS=(
  --start_date "$START_DATE"
  --end_date "$END_DATE"
  --out_lake "$LAKE"
  --project "$PROJECT_ID"
)

case "$RUNNER" in
  local)
    RUNNER_ARGS=(
      --temp_location "$TEMP/beam-local"
    )
    ;;
  dataflow)
    RUNNER_ARGS=(
      --runner DataflowRunner
      --region "$REGION"
      --temp_location "$TEMP/dataflow/temp"
      --staging_location "$TEMP/dataflow/staging"
      --service_account_email "$SA"
      --setup_file ./setup.py
      --no_use_public_ips
      --max_num_workers "${MAX_WORKERS:-2}"
    )
    ;;
  *)
    echo "Unknown runner '$RUNNER'. $USAGE" >&2
    exit 1
    ;;
esac

# Run from etl/ so "python -m beam_etl.pipeline" and ./setup.py resolve
cd "$ETL_DIR"
echo ">> Runner: $RUNNER | $START_DATE -> $END_DATE | lake: $LAKE"
python -m beam_etl.pipeline "${COMMON_ARGS[@]}" "${RUNNER_ARGS[@]}"

# ------------------- CLEANING TRAINING DATA ----------------------
# To run the entire training set post silver validation running "sql_queries/03_validate_silver.sql", run:
# MAX_WORKERS=4 bash etl/scripts/00_run_beam_pipeline.sh $TRAIN_START $TRAIN_END dataflow