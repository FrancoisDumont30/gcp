#!/usr/bin/env bash
# Cloud SQL for PostgreSQL: instance, database, user, then the OLTP schema.
set -euo pipefail
: "${SQL_PASSWORD:?source scripts/env.local.sh first}"

gcloud sql instances create "$SQL_INSTANCE" \
  --region="$REGION" \
  --database-version=POSTGRES_17 \
  --tier=db-f1-micro \
  --storage-size=20GB \
  --edition=ENTERPRISE

gcloud sql databases create "$SQL_DB" --instance="$SQL_INSTANCE"
gcloud sql users create "$SQL_USER" --instance="$SQL_INSTANCE" --password="$SQL_PASSWORD"

# Apply the schema through the Cloud SQL proxy shell (prompts for the password).
gcloud sql connect "$SQL_INSTANCE" --user="$SQL_USER" --database="$SQL_DB" < sql/schema.sql
