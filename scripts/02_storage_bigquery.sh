#!/usr/bin/env bash
# Cloud Storage bucket and BigQuery datasets (raw -> staging -> analytics).
set -euo pipefail
: "${PROJECT_ID:?source scripts/env.local.sh first}"

gcloud storage buckets create "$GCS_BUCKET" --location="$REGION" --uniform-bucket-level-access

gcloud storage cp - "${GCS_BUCKET}/fulfillment/shipments/dt=2026-01-01/README.txt" <<'TXT'
Expected structure:
fulfillment/shipments/dt=YYYY-MM-DD/shipments_*.csv
fulfillment/returns/dt=YYYY-MM-DD/returns_*.csv
TXT

for ds in raw staging analytics; do
  bq --location="$REGION" mk --dataset "${PROJECT_ID}:${ds}"
done
