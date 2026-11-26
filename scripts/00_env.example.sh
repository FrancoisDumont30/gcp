# Copy to scripts/env.local.sh (git-ignored), fill in, then: source scripts/env.local.sh
# Never commit real project, organisation, billing or password values.

export ORGANIZATION="<your-organization-id>"        # optional, only if the project is created under an org
export PROJECT_ID="<your-project-id>"
export BILLING_ACCOUNT_ID="<your-billing-account-id>"
export REGION="europe-west1"

export SA_COMPOSER="sa-composer-de-end2end"
export SA_COMPOSER_EMAIL="${SA_COMPOSER}@${PROJECT_ID}.iam.gserviceaccount.com"

export GCS_BUCKET="gs://de-end2end-${PROJECT_ID}"
export SQL_INSTANCE="projet-end2end-sql"
export SQL_DB="ecom"
export SQL_USER="ecom_user"
# Generate a strong password locally and keep it out of git:
#   export SQL_PASSWORD="$(openssl rand -base64 24)"
export SQL_PASSWORD="<generate-me>"

export COMPOSER_ENV="capstone-composer"
