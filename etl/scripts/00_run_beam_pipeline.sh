#!/usr/bin/env bash
# run_beam_pipeline.sh
# Runs the Beam cleaning pipeline locally for a date range.
# Usage: bash etl/scripts/run_local.sh 2022-01-01 2022-01-02

set -euo pipefail

START_DATE=${1:?Usage: run_local.sh START_DATE END_DATE}
END_DATE=${2:?Usage: run_local.sh START_DATE END_DATE}

# Resolve paths relative to this script, so it works from any folder
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ETL_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$ETL_DIR")"

source "$REPO_DIR/config/dev.env"

# Run from etl/ so "python -m beam_etl.pipeline" can find the package
cd "$ETL_DIR"
python -m beam_etl.pipeline \
  --start_date "$START_DATE" \
  --end_date "$END_DATE" \
  --out_lake "$LAKE" \
  --project "$PROJECT_ID" \
  --temp_location "$TEMP/beam-local"
