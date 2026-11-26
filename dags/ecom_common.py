"""Pure helpers for the e-commerce batch DAG (no Airflow import, easy to unit test)."""
from datetime import datetime, timedelta
from decimal import Decimal

BQ_RAW_DS = "raw"
LOOKBACK_DAYS = 7

# Source columns come from Cloud SQL (PostgreSQL). `updated_at` becomes
# `source_updated_at` in BigQuery, and `ingested_at` is added at load time.
TABLE_SPECS = {
    "customers": {
        "pk": "customer_id",
        "src_cols": ["customer_id", "created_at", "country", "marketing_opt_in", "updated_at"],
        "ddl": """
            customer_id STRING, created_at TIMESTAMP, country STRING,
            marketing_opt_in BOOL, source_updated_at TIMESTAMP, ingested_at TIMESTAMP
        """,
    },
    "orders": {
        "pk": "order_id",
        "src_cols": ["order_id", "customer_id", "order_ts", "status", "currency", "total_amount", "updated_at"],
        "ddl": """
            order_id STRING, customer_id STRING, order_ts TIMESTAMP, status STRING,
            currency STRING, total_amount NUMERIC, source_updated_at TIMESTAMP, ingested_at TIMESTAMP
        """,
    },
    "order_items": {
        "pk": "order_item_id",
        "src_cols": ["order_item_id", "order_id", "sku", "quantity", "unit_price", "updated_at"],
        "ddl": """
            order_item_id STRING, order_id STRING, sku STRING, quantity INT64,
            unit_price NUMERIC, source_updated_at TIMESTAMP, ingested_at TIMESTAMP
        """,
    },
    "payments": {
        "pk": "payment_id",
        "src_cols": ["payment_id", "order_id", "provider", "status", "authorized_ts",
                     "settled_ts", "amount", "updated_at"],
        "ddl": """
            payment_id STRING, order_id STRING, provider STRING, status STRING,
            authorized_ts TIMESTAMP, settled_ts TIMESTAMP, amount NUMERIC,
            source_updated_at TIMESTAMP, ingested_at TIMESTAMP
        """,
    },
}


def bq_col(col: str) -> str:
    """Map a source column name to its BigQuery name."""
    return "source_updated_at" if col == "updated_at" else col


def to_json_value(value):
    """Make a PostgreSQL value JSON serialisable for a BigQuery load job."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def extraction_window(ds: str, lookback_days: int = LOOKBACK_DAYS):
    """Return (start, end) of the extraction window for a logical date `ds` (YYYY-MM-DD).

    The window re-reads the last `lookback_days` days so late updates are picked up;
    the MERGE makes the reload idempotent.
    """
    run_date = datetime.strptime(ds, "%Y-%m-%d")
    return run_date - timedelta(days=lookback_days), run_date + timedelta(days=1)


def build_select_sql(table_name: str) -> str:
    cols = ", ".join(TABLE_SPECS[table_name]["src_cols"])
    return f"""
        SELECT {cols}
        FROM {table_name}
        WHERE updated_at >= %(start_ts)s AND updated_at < %(end_ts)s
    """


def build_merge_sql(target_table: str, load_table: str, pk: str, bq_cols: list) -> str:
    """Upsert from the temporary load table into the raw table.

    A row is only updated when the incoming `source_updated_at` is not older than
    the stored one, so replaying an old run never overwrites newer data.
    """
    non_pk_cols = [c for c in bq_cols if c != pk]
    return f"""
        MERGE `{target_table}` T
        USING `{load_table}` S
        ON T.{pk} = S.{pk}
        WHEN MATCHED AND (T.source_updated_at IS NULL OR S.source_updated_at >= T.source_updated_at) THEN
          UPDATE SET {", ".join(f"{c} = S.{c}" for c in non_pk_cols)}
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(bq_cols)}) VALUES ({", ".join(f"S.{c}" for c in bq_cols)})
        """
