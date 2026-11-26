#!/usr/bin/env bash
# Cloud Composer environment, Airflow connection to Cloud SQL, DAG upload.
set -euo pipefail
: "${SQL_PASSWORD:?source scripts/env.local.sh first}"

gcloud services enable composer.googleapis.com iamcredentials.googleapis.com

gcloud composer environments create "$COMPOSER_ENV" \
  --location="$REGION" \
  --environment-size=small \
  --service-account="$SA_COMPOSER_EMAIL" \
  --image-version=composer-3-airflow-2

# Prefer the private IP or the Cloud SQL connection name over a public IP.
SQL_HOST="$(gcloud sql instances describe "$SQL_INSTANCE" --format='value(ipAddresses[0].ipAddress)')"

gcloud composer environments run "$COMPOSER_ENV" --location "$REGION" \
  connections add -- cloudsql_postgres \
  --conn-type postgres --conn-host "$SQL_HOST" --conn-login "$SQL_USER" \
  --conn-password "$SQL_PASSWORD" --conn-schema "$SQL_DB" --conn-port 5432

DAGS_PREFIX="$(gcloud composer environments describe "$COMPOSER_ENV" --location "$REGION" --format='value(config.dagGcsPrefix)')"
gcloud storage cp dags/*.py "$DAGS_PREFIX/"
