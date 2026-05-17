-- Raw layer: lands as-is from CSV. All STRING. No validation.
-- Partitioned on load_date so reprocessing/replay is cheap.

CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.raw`
OPTIONS (location = '${LOCATION}');

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.raw.orders_raw` (
  order_id        STRING,
  order_date      STRING,
  customer_id     STRING,
  customer_email  STRING,
  product_name    STRING,
  category        STRING,
  quantity        STRING,
  unit_price      STRING,
  revenue         STRING,
  store_name      STRING,
  shipping_city   STRING,
  shipping_state  STRING,
  shipping_zip    STRING,
  -- audit
  source_file     STRING,
  load_date       DATE,
  loaded_at       TIMESTAMP
)
PARTITION BY load_date
OPTIONS (
  description = 'Raw landing zone. CSV-faithful. STRING-only. Audit columns added at load.',
  partition_expiration_days = 90
);
