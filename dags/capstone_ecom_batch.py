"""Daily batch: Cloud SQL (PostgreSQL) -> BigQuery `raw` dataset, orchestrated by Airflow (Cloud Composer).

Flow: create raw tables -> simulate one day of OLTP activity -> incremental load of each
table (load job into a temp table, then MERGE) -> row-count control check.
"""
import logging
from contextlib import closing
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery

from ecom_common import (
    BQ_RAW_DS,
    TABLE_SPECS,
    bq_col,
    build_merge_sql,
    build_select_sql,
    extraction_window,
    to_json_value,
)
from ecom_simulation import build_simulation_statements

log = logging.getLogger(__name__)

PG_CONN_ID = "cloudsql_postgres"
BQ_CONN_ID = "google_cloud_default"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def simulate_oltp_activity(ds, **context):
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with closing(pg.get_conn()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for stmt in build_simulation_statements(ds):
                cur.execute(stmt)


def create_raw_tables(**context):
    """Create the raw tables if needed (the `raw` dataset must already exist)."""
    client = BigQueryHook(gcp_conn_id=BQ_CONN_ID).get_client()
    for table_name, spec in TABLE_SPECS.items():
        client.query(
            f"CREATE TABLE IF NOT EXISTS `{client.project}.{BQ_RAW_DS}.{table_name}` ({spec['ddl']})"
        ).result()


def extract_and_merge(table_name: str, ds: str, **context):
    """Extract a table from PostgreSQL, load it into a temp BigQuery table, then MERGE."""
    spec = TABLE_SPECS[table_name]
    pk = spec["pk"]
    src_cols = spec["src_cols"]

    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    client = BigQueryHook(gcp_conn_id=BQ_CONN_ID).get_client()
    project_id = client.project

    start_ts, end_ts = extraction_window(ds)
    rows = pg.get_records(build_select_sql(table_name), parameters={"start_ts": start_ts, "end_ts": end_ts})
    log.info("%s: %d rows extracted between %s and %s", table_name, len(rows), start_ts, end_ts)

    if not rows:
        log.warning("%s: no rows in the window, nothing to load", table_name)
        return

    bq_cols = [bq_col(c) for c in src_cols] + ["ingested_at"]
    now_ts = datetime.now(timezone.utc).isoformat()
    load_rows = [
        {**{bq_col(c): to_json_value(v) for c, v in zip(src_cols, r)}, "ingested_at": now_ts}
        for r in rows
    ]

    target_table = f"{project_id}.{BQ_RAW_DS}.{table_name}"
    load_table = f"{project_id}.{BQ_RAW_DS}.{table_name}__load_{ds.replace('-', '')}"

    # Load job (no streaming) using the schema of the target table.
    target_schema = client.get_table(target_table).schema
    client.load_table_from_json(
        load_rows,
        load_table,
        job_config=bigquery.LoadJobConfig(
            schema=target_schema,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        ),
    ).result()

    try:
        client.query(build_merge_sql(target_table, load_table, pk, bq_cols)).result()
        log.info("%s: MERGE done (%d rows)", table_name, len(load_rows))
    finally:
        client.delete_table(load_table, not_found_ok=True)


def bq_control_check(**context):
    client = BigQueryHook(gcp_conn_id=BQ_CONN_ID).get_client()
    for table_name in TABLE_SPECS:
        n = list(client.query(
            f"SELECT COUNT(*) AS n FROM `{client.project}.{BQ_RAW_DS}.{table_name}`"
        ).result())[0]["n"]
        log.info("raw.%s: %d rows", table_name, n)


with DAG(
    dag_id="capstone_ecom_batch_v1",
    start_date=datetime(2026, 11, 1),  # recent date: avoids hundreds of backfill runs
    schedule="@daily",                 # `schedule_interval` was removed in Airflow 3
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["capstone", "batch", "cloudsql", "bigquery"],
) as dag:

    t_create_tables = PythonOperator(task_id="create_raw_tables", python_callable=create_raw_tables)
    t_simulate_oltp = PythonOperator(task_id="simulate_oltp_activity", python_callable=simulate_oltp_activity)

    load_tasks = {
        name: PythonOperator(
            task_id=f"load_{name}_raw",
            python_callable=extract_and_merge,
            op_kwargs={"table_name": name},
        )
        for name in TABLE_SPECS
    }

    t_bq_control = PythonOperator(task_id="bq_control_check", python_callable=bq_control_check)

    t_create_tables >> t_simulate_oltp >> load_tasks["customers"]
    load_tasks["customers"] >> load_tasks["orders"] >> load_tasks["order_items"]
    load_tasks["orders"] >> load_tasks["payments"]
    [load_tasks["order_items"], load_tasks["payments"]] >> t_bq_control
