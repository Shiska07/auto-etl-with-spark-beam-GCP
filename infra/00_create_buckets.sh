#!/usr/bin/env bash

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
      --uniform-bucket-level-access
    echo "    created: $BUCKET"
  fi
done

echo ">>> Enabling versioning on the lake bucket..."
# If a job overwrites or deletes data by mistake, older versions are kept.
gcloud storage buckets update "$LAKE" --versioning

echo ">>> Adding a 7-day auto-delete rule to the temp bucket..."
# Dataflow and Dataproc leave staging files behind. This cleans them up automatically.
LIFECYCLE_FILE=$(mktemp)
cat > "$LIFECYCLE_FILE" <<'EOF'
{
  "rule": [
    { "action": { "type": "Delete" }, "condition": { "age": 7 } }
  ]
}
EOF
gcloud storage buckets update "$TEMP" --lifecycle-file="$LIFECYCLE_FILE"
rm -f "$LIFECYCLE_FILE"

echo ">>> Done. Buckets in this project:"
gcloud storage ls
