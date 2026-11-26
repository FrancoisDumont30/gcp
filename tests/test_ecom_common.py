import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from ecom_common import (  # noqa: E402
    TABLE_SPECS, bq_col, build_merge_sql, build_select_sql, extraction_window, to_json_value,
)
from ecom_simulation import build_simulation_statements  # noqa: E402


def test_bq_col_renames_updated_at_only():
    assert bq_col("updated_at") == "source_updated_at"
    assert bq_col("order_id") == "order_id"


def test_to_json_value():
    assert to_json_value(None) is None
    assert to_json_value(Decimal("12.50")) == 12.5
    assert to_json_value(datetime(2026, 11, 1, 12, 0)) == "2026-11-01T12:00:00"
    assert to_json_value("x") == "x"


def test_extraction_window_covers_lookback_and_run_day():
    start, end = extraction_window("2026-11-10", lookback_days=7)
    assert start == datetime(2026, 11, 3)
    assert end == datetime(2026, 11, 11)


def test_specs_are_consistent():
    for name, spec in TABLE_SPECS.items():
        assert spec["pk"] in spec["src_cols"], name
        for col in spec["src_cols"]:
            assert bq_col(col) in spec["ddl"], (name, col)
        assert "ingested_at" in spec["ddl"]


def test_select_sql_is_incremental():
    sql = build_select_sql("orders")
    assert "FROM orders" in sql and "updated_at >= %(start_ts)s" in sql


def test_merge_sql_does_not_overwrite_newer_rows():
    cols = ["order_id", "status", "source_updated_at", "ingested_at"]
    sql = build_merge_sql("p.raw.orders", "p.raw.orders__load_x", "order_id", cols)
    assert "ON T.order_id = S.order_id" in sql
    assert "S.source_updated_at >= T.source_updated_at" in sql
    assert "order_id = S.order_id" not in sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]


def test_simulation_is_deterministic_and_date_based():
    a = build_simulation_statements("2026-11-05")
    assert a == build_simulation_statements("2026-11-05")
    assert all("NOW()" not in s.upper() for s in a)
    assert any("c_2026-11-05_" in s for s in a)
