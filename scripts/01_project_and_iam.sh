#!/usr/bin/env bash
# Create the GCP project, link billing, and create the Composer service account with least-privilege roles.
set -euo pipefail
: "${PROJECT_ID:?source scripts/env.local.sh first}"

gcloud projects create "$PROJECT_ID" --name="my gcp project" ${ORGANIZATION:+--organization="$ORGANIZATION"}
gcloud config set project "$PROJECT_ID"
gcloud config set compute/region "$REGION"
gcloud billing projects link "$PROJECT_ID" --billing-account="$BILLING_ACCOUNT_ID"

gcloud iam service-accounts create "$SA_COMPOSER" \
  --description="Service account for Cloud Composer" \
  --display-name="Composer Service Account" \
  --project="$PROJECT_ID"

for role in roles/bigquery.jobUser roles/bigquery.dataEditor roles/storage.objectViewer roles/cloudsql.client; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SA_COMPOSER_EMAIL" --role="$role"
done
