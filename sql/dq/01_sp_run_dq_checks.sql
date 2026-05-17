-- Stored procedure: runs all DQ checks for a given load_date.
-- Called by the gcs_loader Cloud Function after the raw load.
-- Pure BigQuery SQL. No external orchestrator.
--
-- Strategy:
--   1. Stage typed/cast view of the day's raw rows.
--   2. Tag each row with an array of failure_reasons (BLOCKING) and dq_flags (NON-BLOCKING).
--   3. Rows with ANY blocking reason -> dead_letter.
--   4. Rows with no blocking reasons -> silver.orders (with dq_flags).
--   5. Idempotent: deletes the partition for load_date in both targets before inserting.

CREATE OR REPLACE PROCEDURE `${PROJECT_ID}.silver.sp_run_dq_checks`(IN p_load_date DATE)
BEGIN
  DECLARE rows_loaded INT64;
  DECLARE rows_rejected INT64;

  -- 1. Idempotency: clear any prior attempt for this load_date.
  DELETE FROM `${PROJECT_ID}.silver.orders`
  WHERE load_date = p_load_date;

  DELETE FROM `${PROJECT_ID}.silver.orders_dead_letter`
  WHERE load_date = p_load_date;

  -- 2. Stage: cast + dedupe-aware view of today's raw rows.
  --    Keep first occurrence per (order_id, product_name) — duplicate rows in same file = blocking issue,
  --    but we keep ONE copy (the earliest by ordinal) so the order isn't lost entirely.
  CREATE TEMP TABLE staged AS
  WITH typed AS (
    SELECT
      order_id,
      SAFE.PARSE_DATE('%Y-%m-%d', order_date)         AS order_date_typed,
      customer_id,
      LOWER(TRIM(customer_email))                     AS customer_email,
      INITCAP(TRIM(product_name))                     AS product_name,   -- normalize "iphone 14" / "IPHONE 14" / " iPhone 14 "
      INITCAP(TRIM(category))                         AS category,
      SAFE_CAST(quantity   AS INT64)                  AS quantity,
      SAFE_CAST(unit_price AS NUMERIC)                AS unit_price,
      SAFE_CAST(revenue    AS NUMERIC)                AS revenue,
      TRIM(store_name)                                AS store_name,
      TRIM(shipping_city)                             AS shipping_city,
      UPPER(TRIM(shipping_state))                     AS shipping_state,
      TRIM(shipping_zip)                              AS shipping_zip,
      source_file,
      load_date,
      loaded_at,
      -- preserve the raw row for the dead letter table
      TO_JSON(t)                                      AS raw_row,
      ROW_NUMBER() OVER (
        PARTITION BY order_id, product_name
        ORDER BY loaded_at
      ) AS rn_dup
    FROM `${PROJECT_ID}.raw.orders_raw` AS t
    WHERE load_date = p_load_date
  )
  SELECT
    *,
    -- BLOCKING reasons: row will go to dead letter if any are non-empty
    ARRAY(
      SELECT reason FROM UNNEST([
        IF(order_id IS NULL OR order_id = '',                'missing_order_id', NULL),
        IF(order_date_typed IS NULL,                          'invalid_or_missing_order_date', NULL),
        IF(quantity IS NULL OR quantity <= 0,                 'invalid_quantity', NULL),
        IF(unit_price IS NULL OR unit_price < 0,              'invalid_unit_price', NULL),
        IF(revenue IS NULL OR revenue < 0,                    'invalid_revenue', NULL),
        IF(store_name IS NULL OR store_name = '',             'missing_store_name', NULL),
        IF(product_name IS NULL OR product_name = '',         'missing_product_name', NULL),
        IF(rn_dup > 1,                                        'duplicate_order_product_in_file', NULL)
      ]) AS reason
      WHERE reason IS NOT NULL
    ) AS failure_reasons,

    -- NON-BLOCKING flags: row is good enough for silver, but flag it
    ARRAY(
      SELECT flag FROM UNNEST([
        -- revenue should equal qty * unit_price within 1c tolerance
        IF(revenue IS NOT NULL
           AND unit_price IS NOT NULL
           AND quantity IS NOT NULL
           AND ABS(revenue - (quantity * unit_price)) > 0.01,
           'revenue_mismatch_qty_x_price', NULL),
        IF(customer_email IS NULL OR NOT REGEXP_CONTAINS(customer_email, r'^[^@\s]+@[^@\s]+\.[^@\s]+$'),
           'invalid_email_format', NULL),
        IF(shipping_state IS NOT NULL AND LENGTH(shipping_state) <> 2,
           'non_standard_state_code', NULL),
        IF(shipping_zip IS NOT NULL AND NOT REGEXP_CONTAINS(shipping_zip, r'^\d{5}(-\d{4})?$'),
           'invalid_zip_format', NULL),
        IF(order_date_typed > CURRENT_DATE(),
           'future_dated_order', NULL)
      ]) AS flag
      WHERE flag IS NOT NULL
    ) AS dq_flags
  FROM typed;

  -- 3. Cross-file dedupe: order_id already exists in silver (different load_date).
  --    Add as a BLOCKING reason via UPDATE.
  UPDATE staged s
  SET failure_reasons = ARRAY_CONCAT(s.failure_reasons, ['duplicate_order_id_seen_previously'])
  WHERE EXISTS (
    SELECT 1
    FROM `${PROJECT_ID}.silver.orders` o
    WHERE o.order_id = s.order_id
      AND o.load_date <> p_load_date  -- redundant given step 1, but safe
  );

  -- 4. Dead letter insert: any row with at least one blocking reason.
  INSERT INTO `${PROJECT_ID}.silver.orders_dead_letter`
    (raw_row, failure_reasons, source_file, load_date, rejected_at)
  SELECT
    raw_row,
    failure_reasons,
    source_file,
    load_date,
    CURRENT_TIMESTAMP()
  FROM staged
  WHERE ARRAY_LENGTH(failure_reasons) > 0;

  -- 5. Silver insert: clean rows + non-blocking flags.
  INSERT INTO `${PROJECT_ID}.silver.orders` (
    order_id, order_date, customer_id, customer_email,
    product_name, category, quantity, unit_price, revenue,
    store_name, shipping_city, shipping_state, shipping_zip,
    dq_flags, source_file, load_date, loaded_at
  )
  SELECT
    order_id, order_date_typed, customer_id, customer_email,
    product_name, category, quantity, unit_price, revenue,
    store_name, shipping_city, shipping_state, shipping_zip,
    dq_flags, source_file, load_date, loaded_at
  FROM staged
  WHERE ARRAY_LENGTH(failure_reasons) = 0;

  -- 6. Logging counters (visible in stored proc execution result).
  SET rows_loaded   = (SELECT COUNT(*) FROM `${PROJECT_ID}.silver.orders` WHERE load_date = p_load_date);
  SET rows_rejected = (SELECT COUNT(*) FROM `${PROJECT_ID}.silver.orders_dead_letter` WHERE load_date = p_load_date);

  -- Surface as a single-row result for the caller (Cloud Function logs it).
  SELECT
    p_load_date     AS load_date,
    rows_loaded     AS rows_loaded_to_silver,
    rows_rejected   AS rows_to_dead_letter,
    SAFE_DIVIDE(rows_rejected, rows_loaded + rows_rejected) AS reject_rate;
END;
