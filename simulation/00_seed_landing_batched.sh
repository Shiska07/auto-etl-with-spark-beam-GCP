#!/usr/bin/env bash
# 00_seed_landing_batched.sh
# Seeds bronze landing data for the holdout window, split into what production
# would receive: trip requests (no tip) and ground truth (tip arrives later).
# Usage: bash simulation/00_seed_landing.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
source "$REPO_DIR/config/dev.env"

: "${PROJECT_ID:?}" "${LAKE:?}" "${HOLDOUT_START:?}" "${HOLDOUT_END:?}"

STAGING="$PROJECT_ID.taxi_lake.holdout_raw"

# 1) One scan of the public table → small staging table (raw, unfiltered)
bq query --use_legacy_sql=false --project_id="$PROJECT_ID" "
CREATE OR REPLACE TABLE \`$STAGING\` AS
SELECT *
FROM \`bigquery-public-data.chicago_taxi_trips.taxi_trips\`
WHERE trip_start_timestamp >= TIMESTAMP('$HOLDOUT_START')
  AND trip_start_timestamp <  TIMESTAMP('$HOLDOUT_END')"

# 2) Export one folder per day, from the small table
d="$HOLDOUT_START"
while [[ "$d" < "$HOLDOUT_END" ]]; do
  echo ">> $d"

  # Requests: everything known at payment time (no tips, no trip_total)
  bq query --use_legacy_sql=false --project_id="$PROJECT_ID" "
  EXPORT DATA OPTIONS (
    uri = '$LAKE/landing/live_trips/event_date=$d/part-*.json',
    format = 'JSON', overwrite = true)
  AS SELECT * EXCEPT (tips, trip_total)
  FROM \`$STAGING\`
  WHERE DATE(trip_start_timestamp) = '$d'"

  # Ground truth: the outcome, joined back later on unique_key
  bq query --use_legacy_sql=false --project_id="$PROJECT_ID" "
  EXPORT DATA OPTIONS (
    uri = '$LAKE/landing/ground_truth/event_date=$d/part-*.json',
    format = 'JSON', overwrite = true)
  AS SELECT unique_key, tips, trip_total, trip_end_timestamp
  FROM \`$STAGING\`
  WHERE DATE(trip_start_timestamp) = '$d'"

  d=$(date -I -d "$d + 1 day")
done

echo ">> Done. Landing data under $LAKE/landing/"
