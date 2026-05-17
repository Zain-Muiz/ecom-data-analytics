-- Silver layer: typed, deduped, with DQ flags.
-- Partitioned on order_date (the natural query dimension), clustered on store_name.

CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.silver`
OPTIONS (location = '${LOCATION}');

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.silver.orders` (
  order_id         STRING NOT NULL,
  order_date       DATE   NOT NULL,
  customer_id      STRING,
  customer_email   STRING,
  product_name     STRING,
  category         STRING,
  quantity         INT64,
  unit_price       NUMERIC,
  revenue          NUMERIC,
  store_name       STRING,
  shipping_city    STRING,
  shipping_state   STRING,
  shipping_zip     STRING,
  -- DQ flags: row passed all checks, but here are non-blocking quality signals
  dq_flags         ARRAY<STRING>,
  -- audit
  source_file      STRING,
  load_date        DATE,
  loaded_at        TIMESTAMP
)
PARTITION BY order_date
CLUSTER BY store_name
OPTIONS (
  description = 'Cleaned, typed orders. dq_flags carry non-fatal data quality signals.',
  require_partition_filter = FALSE
);

-- Dead letter: anything that fails *blocking* DQ checks. Strictly never reaches marts.
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.silver.orders_dead_letter` (
  raw_row          JSON,         -- entire raw row as JSON for forensics + replay
  failure_reasons  ARRAY<STRING>, -- one or more reasons
  source_file      STRING,
  load_date        DATE,
  rejected_at      TIMESTAMP
)
PARTITION BY load_date
OPTIONS (
  description = 'Rows rejected by blocking DQ checks. Inspect, fix at source, replay if needed.'
);
