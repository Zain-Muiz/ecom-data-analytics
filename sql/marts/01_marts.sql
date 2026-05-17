-- Marts: narrow, fast, cheap. The chatbot queries ONLY here.
-- Built from silver.orders (the source of truth).
-- Refreshed via scheduled query (hourly).

CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.marts`
OPTIONS (location = '${LOCATION}');

-- 1. Daily revenue rollup (the most common chatbot question)
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.marts.fct_daily_revenue` (
  order_date     DATE   NOT NULL,
  store_name     STRING NOT NULL,
  category       STRING,
  orders         INT64,
  units          INT64,
  revenue        NUMERIC,
  unique_customers INT64
)
PARTITION BY order_date
CLUSTER BY store_name
OPTIONS (description = 'Daily store/category revenue rollup. Primary surface for chatbot summary questions.');

-- 2. Product dimension
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.marts.dim_product` (
  product_name      STRING NOT NULL,
  category          STRING,
  total_units_sold  INT64,
  total_revenue     NUMERIC,
  first_sold_date   DATE,
  last_sold_date    DATE
)
OPTIONS (description = 'Product dimension with lifetime aggregates. Used for "best-selling" questions.');

-- 3. Store dimension
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.marts.dim_store` (
  store_name        STRING NOT NULL,
  total_orders      INT64,
  total_revenue     NUMERIC,
  first_order_date  DATE,
  last_order_date   DATE
)
OPTIONS (description = 'Store performance dimension.');

-- 4. Order grain fact (full granularity, for drill-down questions)
CREATE OR REPLACE VIEW `${PROJECT_ID}.marts.fct_orders` AS
SELECT
  order_id,
  order_date,
  customer_id,
  product_name,
  category,
  quantity,
  unit_price,
  revenue,
  store_name,
  shipping_state
FROM `${PROJECT_ID}.silver.orders`;

-- ===== Refresh procedure =====
-- Called by a scheduled query, hourly.
CREATE OR REPLACE PROCEDURE `${PROJECT_ID}.marts.sp_refresh_marts`()
BEGIN
  -- fct_daily_revenue: full rebuild (cheap at this scale)
  TRUNCATE TABLE `${PROJECT_ID}.marts.fct_daily_revenue`;

  INSERT INTO `${PROJECT_ID}.marts.fct_daily_revenue`
  SELECT
    order_date,
    store_name,
    category,
    COUNT(DISTINCT order_id)    AS orders,
    SUM(quantity)               AS units,
    SUM(revenue)                AS revenue,
    COUNT(DISTINCT customer_id) AS unique_customers
  FROM `${PROJECT_ID}.silver.orders`
  GROUP BY order_date, store_name, category;

  -- dim_product
  TRUNCATE TABLE `${PROJECT_ID}.marts.dim_product`;
  INSERT INTO `${PROJECT_ID}.marts.dim_product`
  SELECT
    product_name,
    ANY_VALUE(category)  AS category,
    SUM(quantity)        AS total_units_sold,
    SUM(revenue)         AS total_revenue,
    MIN(order_date)      AS first_sold_date,
    MAX(order_date)      AS last_sold_date
  FROM `${PROJECT_ID}.silver.orders`
  GROUP BY product_name;

  -- dim_store
  TRUNCATE TABLE `${PROJECT_ID}.marts.dim_store`;
  INSERT INTO `${PROJECT_ID}.marts.dim_store`
  SELECT
    store_name,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(revenue)             AS total_revenue,
    MIN(order_date)          AS first_order_date,
    MAX(order_date)          AS last_order_date
  FROM `${PROJECT_ID}.silver.orders`
  GROUP BY store_name;
END;
