"""SQL that simulates one day of OLTP activity in Cloud SQL.

Timestamps depend on the logical date of the run (`ds`), never on NOW(), so the DAG
can be replayed (catchup / backfill) and produces the same data.
"""
from ecom_common import LOOKBACK_DAYS


def build_simulation_statements(run_date: str, lookback_days: int = LOOKBACK_DAYS) -> list:
    day_start = f"'{run_date}'::timestamp"
    return [
        f"""
        INSERT INTO customers (customer_id, created_at, country, marketing_opt_in, updated_at)
        SELECT
          'c_{run_date}_' || LPAD(gs::text, 4, '0'),
          {day_start} + (gs * INTERVAL '1 second'),
          (ARRAY['US','CA','FR','DE','GB'])[1 + (gs % 5)],
          (gs % 2 = 0),
          {day_start} + (gs * INTERVAL '1 second')
        FROM generate_series(1, 50) gs
        ON CONFLICT (customer_id) DO NOTHING;
        """,
        f"""
        INSERT INTO orders (order_id, customer_id, order_ts, status, currency, total_amount, updated_at)
        SELECT
          'o_{run_date}_' || LPAD(gs::text, 5, '0'),
          'c_{run_date}_' || LPAD(((gs % 50) + 1)::text, 4, '0'),
          {day_start} + ((gs % 3600) * INTERVAL '1 second'),
          'CREATED',
          'USD',
          (20 + (gs % 200))::numeric(12,2),
          {day_start} + ((gs % 3600) * INTERVAL '1 second')
        FROM generate_series(1, 200) gs
        ON CONFLICT (order_id) DO NOTHING;
        """,
        f"""
        INSERT INTO order_items (order_item_id, order_id, sku, quantity, unit_price, updated_at)
        SELECT
          'oi_{run_date}_' || LPAD(gs::text, 5, '0') || '_' || item_n,
          'o_{run_date}_' || LPAD(gs::text, 5, '0'),
          'SKU-' || LPAD(((gs % 100) + 1)::text, 4, '0'),
          1 + (gs % 3),
          (5 + (gs % 50))::numeric(12,2),
          {day_start} + ((gs % 3600) * INTERVAL '1 second')
        FROM generate_series(1, 200) gs
        CROSS JOIN (VALUES ('A'), ('B')) AS it(item_n)
        ON CONFLICT (order_item_id) DO NOTHING;
        """,
        f"""
        INSERT INTO payments (payment_id, order_id, provider, status, authorized_ts, settled_ts, amount, updated_at)
        SELECT
          'p_{run_date}_' || LPAD(gs::text, 5, '0'),
          'o_{run_date}_' || LPAD(gs::text, 5, '0'),
          (ARRAY['stripe','adyen'])[1 + (gs % 2)],
          'AUTHORIZED',
          {day_start} + ((gs % 3600) * INTERVAL '1 second'),
          NULL,
          (20 + (gs % 200))::numeric(12,2),
          {day_start} + ((gs % 3600) * INTERVAL '1 second')
        FROM generate_series(1, 200) gs
        WHERE (gs % 10) < 7
        ON CONFLICT (payment_id) DO NOTHING;
        """,
        f"""
        UPDATE payments
        SET status = 'SETTLED',
            settled_ts = {day_start} + INTERVAL '12 hours',
            updated_at = {day_start} + INTERVAL '12 hours'
        WHERE status = 'AUTHORIZED'
          AND updated_at >= {day_start} - INTERVAL '{lookback_days} days'
          AND updated_at <  {day_start} + INTERVAL '1 day'
          AND (substring(payment_id from '.{{2}}$')::int % 20) = 0;
        """,
        f"""
        UPDATE orders
        SET status = 'CANCELLED',
            updated_at = {day_start} + INTERVAL '12 hours'
        WHERE status = 'CREATED'
          AND updated_at >= {day_start} - INTERVAL '{lookback_days} days'
          AND updated_at <  {day_start} + INTERVAL '1 day'
          AND (substring(order_id from '.{{2}}$')::int % 50) = 0;
        """,
    ]
