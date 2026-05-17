# Technical Design Document: E-Commerce BI Platform

**Version:** 1.0  
**Date:** May 18, 2026  
**Status:** Active  
**Owner:** BI Engineering Team

---

## Table of Contents

1. [Overview](#overview)
2. [Data Model and Design Choices](#data-model-and-design-choices)
3. [Architecture and Pipeline Flow](#architecture-and-pipeline-flow)
4. [Design Principles](#design-principles)
5. [Assumptions](#assumptions)
6. [Implementation Details](#implementation-details)
7. [Operational Considerations](#operational-considerations)

---

## Overview

### Purpose

The E-Commerce BI Platform automates daily e-commerce order ingestion from email attachments into BigQuery, applies comprehensive data quality checks, and serves a self-service AI chatbot for marketing analytics queries.

### Problem Statement

The BI team faced three critical challenges:

1. **Manual Ingestion Burden** — Daily CSV attachments in email required manual download and BigQuery loading
2. **Data Quality Blindness** — Bad data reached reports and dashboards undetected
3. **Limited Self-Service** — Marketing analysts waited for BI team to run ad-hoc queries

### Solution Architecture

A minimal, serverless stack combining five GCP services:

- **Google Apps Script** — Email polling and attachment extraction (runs in inbox owner's account)
- **Cloud Storage (GCS)** — Staging for incoming CSVs and long-term archive
- **Cloud Functions** — Event-driven pipeline orchestration (object.finalize trigger)
- **BigQuery** — Raw ingestion, SQL-based DQ, staging, and analytics marts
- **Cloud Run + Vertex AI** — ADK chatbot for natural-language analytics

**Key Philosophy:** Minimal viable services that solve the problem end-to-end. No Dataflow, Composer, Cloud Scheduler, or Secret Manager.

### Core Metrics

| Metric               | Target                  | Current  |
| -------------------- | ----------------------- | -------- |
| Ingestion latency    | < 5 minutes from email  | Achieved |
| DQ failure detection | 100% of blocking issues | Achieved |
| Data freshness       | Hourly marts refresh    | Achieved |
| Cost per load        | < $0.50                 | Achieved |
| Uptime SLA           | 99%                     | Achieved |

---

## Data Model and Design Choices

### Data Modeling Approach

#### 1. Star Schema Design

The data warehouse follows a lightweight star schema optimized for chatbot queries and self-service analytics:

```
                    ┌──────────────────────────────┐
                    │   fct_daily_revenue          │
                    │  (fact table)                │
                    ├──────────────────────────────┤
                    │ order_date (PK)              │
                    │ store_name (PK, FK)          │
                    │ category (PK)                │
                    │ orders (count)               │
                    │ units (sum qty)              │
                    │ revenue (sum)                │
                    │ unique_customers (count)     │
                    └──────────────────────────────┘
                           ▲           ▲
                           │           │
                ┌──────────┘           └──────────┐
                │                                 │
         ┌──────────────────┐            ┌─────────────────┐
         │  dim_store       │            │  dim_product    │
         ├──────────────────┤            ├─────────────────┤
         │ store_name (PK)  │            │ product_name(PK)│
         │ total_orders     │            │ category        │
         │ total_revenue    │            │ total_units_sold│
         │ first_order_date │            │ total_revenue   │
         │ last_order_date  │            │ first_sold_date │
         │                  │            │ last_sold_date  │
         └──────────────────┘            └─────────────────┘

         Also provides: fct_orders (view for drill-down)
```

**Rationale:**

- **Fact table** (`fct_daily_revenue`): Pre-aggregated at the day/store/category grain, optimized for the most common chatbot queries ("Which store sold the most yesterday?")
- **Dimensions**: Denormalized for query simplicity (no JOINs required for chatbot responses)
- **Drill-down view** (`fct_orders`): Full-grain transactions for granular investigations

#### 2. Slowly Changing Dimensions (Type 2)

**Product Dimension** uses implicit SCD Type 2 via temporal clustering:

- Products are identified by `product_name` and `category`
- Product names are normalized at staging (`INITCAP(TRIM(...))`) so "iPhone 14", "IPHONE 14", "iphone 14" collapse to one product
- If a product's category changes, it appears as a new row with updated category but same product_name
- Historical aggregates are preserved: `first_sold_date`, `last_sold_date` track lifecycle

**Store Dimension** similarly tracks:

- `first_order_date`: first order ever placed at this store
- `last_order_date`: most recent order timestamp
- Total lifetime metrics: `total_orders`, `total_revenue`

**Why not full SCD2 (with start_date/end_date)?**

- At this scale (hundreds of products, dozens of stores), the complexity isn't justified
- Temporal tracking is implicit in date columns and first/last indicators
- Future expansion to full SCD2 is straightforward (add surr. keys, effective_date columns)

#### 3. Date Dimension Strategy

BigQuery's native DATE type is used instead of a dedicated date dimension:

- `order_date` in fact tables and dimensions is `DATE` (not integer surrogate key)
- Partitioning on `order_date` and `load_date` enables partition elimination in queries
- Clustering on `store_name` further accelerates drill-downs by store

**Trade-off:**

- Lost the ability to attach holiday flags or fiscal calendars as dimension attributes
- **Mitigation:** These can be added as STRUCT fields or a separate small table if needed
- **Benefit:** Simplified schema, lower storage cost, BigQuery's native date filtering is fast

---

## Architecture and Pipeline Flow

### 1. Raw Ingestion

**Trigger Path:**

```
Daily Email (9-10 AM)
    ↓
Apps Script pollForEmail (time-triggered every 5 min in 9-10 AM window)
    ↓
Match sender + subject filter
    ↓
Extract CSV attachment + label email
    ↓
Upload via UrlFetchApp to gs://ecom-bi-landing-prod/incoming/orders_YYYYMMDD.csv
    ↓
Apps Script marks thread with label "orders-csv-processed"
```

**Raw Table Schema** (`raw.orders_raw`):

- **All STRING columns** — faithful representation of CSV with zero type coercion
- **Audit columns added at load:**
  - `source_file`: GCS object path
  - `load_date`: DATE partition key (extracted from filename: `orders_YYYYMMDD.csv` → `YYYY-MM-DD`)
  - `loaded_at`: TIMESTAMP when inserted
- **Partitioning:** `PARTITION BY load_date` (90-day retention for cost control)
- **Rationale:** String-only raw data allows replaying with different type coercions if DQ rules change

**Load Mechanism** (Cloud Function `gcs_loader`):

```sql
INSERT INTO `${PROJECT_ID}.raw.orders_raw` (SELECT * FROM
  EXTERNAL_QUERY_SOURCE(
    format = 'CSV',
    uris = ['gs://...orders_YYYYMMDD.csv'],
    skip_leading_rows = 1
  )
)
```

**Idempotency:** If the same file is re-uploaded, the partition is cleared and re-inserted (whole partition replay).

---

### 2. Staging Layer (Data Quality & Type Casting)

**Stored Procedure:** `silver.sp_run_dq_checks(p_load_date DATE)`

**Execution Flow:**

```
                      ┌─────────────────────┐
                      │ sp_run_dq_checks    │
                      │ (called by CF)       │
                      └────────┬────────────┘
                               │
                      ┌────────▼─────────┐
                      │ 1. Clear idempotent
                      │    DELETE silver.*
                      │    for load_date
                      └────────┬─────────┘
                               │
                      ┌────────▼────────────┐
                      │ 2. Stage + Cast     │
                      │    Type conversion  │
                      │    Normalization    │
                      │    Dedupe (in-file) │
                      └────────┬────────────┘
                               │
                      ┌────────▼─────────────────┐
                      │ 3. Tag rows with:        │
                      │    - failure_reasons     │
                      │    - dq_flags            │
                      └────────┬──────────────────┘
                               │
              ┌────────────────┴──────────────────┐
              │                                   │
        ┌─────▼──────────┐             ┌─────────▼────┐
        │ failure_reasons │             │ dq_flags     │
        │ NOT EMPTY?      │             │ (non-blocking)
        │ YES → REJECT    │             │              │
        └─────┬──────────┘             └─────────┬────┘
              │                              │
        ┌─────▼──────────────┐          ┌────▼──────────────┐
        │ silver.orders_     │          │ silver.orders     │
        │ dead_letter        │          │ (with dq_flags)   │
        │ (raw_row JSON,     │          │ (typed, clean)    │
        │  failure_reasons)  │          │                   │
        └────────────────────┘          └────────────────────┘
```

**Processing Steps:**

1. **Idempotency Checkpoint:**

   ```sql
   DELETE FROM silver.orders WHERE load_date = p_load_date;
   DELETE FROM silver.orders_dead_letter WHERE load_date = p_load_date;
   ```

   Replaying a day's data cleanly rebuilds both tables.

2. **Type Casting & Normalization:**
   - `order_date`: `SAFE.PARSE_DATE('%Y-%m-%d', ...)` — NULL if unparseable
   - `customer_email`: `LOWER(TRIM(...))` — normalize case and whitespace
   - `product_name` & `category`: `INITCAP(TRIM(...))` — Collapse "iPhone 14" / "iphone 14" / "IPHONE 14"
   - `quantity`, `unit_price`, `revenue`: `SAFE_CAST(... AS INT64/NUMERIC)` — NULL if invalid
   - `shipping_state`: `UPPER(TRIM(...))` — standardize state codes
   - `shipping_zip`: `TRIM(...)` — removed leading/trailing spaces

3. **Deduplication (Within-File):**

   ```sql
   ROW_NUMBER() OVER (PARTITION BY order_id, product_name ORDER BY loaded_at) AS rn_dup
   ```

   If an order appears twice in the same file, `rn_dup > 1` triggers the `duplicate_order_product_in_file` blocking check.
   The first occurrence is kept (rn_dup = 1) so the order isn't lost entirely.

4. **Blocking Checks** (row → dead letter if ANY match):

| Check                                 | Trigger                                     | Reason Code                          |
| ------------------------------------- | ------------------------------------------- | ------------------------------------ |
| `order_id` null/empty                 | `order_id IS NULL OR order_id = ''`         | `missing_order_id`                   |
| `order_date` invalid/null             | `order_date_typed IS NULL`                  | `invalid_or_missing_order_date`      |
| `quantity` ≤ 0 or null                | `quantity IS NULL OR quantity <= 0`         | `invalid_quantity`                   |
| `unit_price` < 0 or null              | `unit_price IS NULL OR unit_price < 0`      | `invalid_unit_price`                 |
| `revenue` < 0 or null                 | `revenue IS NULL OR revenue < 0`            | `invalid_revenue`                    |
| `store_name` missing                  | `store_name IS NULL OR store_name = ''`     | `missing_store_name`                 |
| `product_name` missing                | `product_name IS NULL OR product_name = ''` | `missing_product_name`               |
| Duplicate (order_id, product) in file | `rn_dup > 1`                                | `duplicate_order_product_in_file`    |
| `order_id` seen before (cross-file)   | Lookup in `silver.orders`                   | `duplicate_order_id_seen_previously` |

5. **Non-Blocking Flags** (row → silver with flag):

| Check                       | Condition                                                   | Flag                           |
| --------------------------- | ----------------------------------------------------------- | ------------------------------ |
| Revenue arithmetic mismatch | `ABS(revenue - qty × unit_price) > 0.01`                    | `revenue_mismatch_qty_x_price` |
| Invalid email format        | `NOT REGEXP_CONTAINS(email, r'^[^@\s]+@[^@\s]+\.[^@\s]+$')` | `invalid_email_format`         |
| Non-standard state code     | `LENGTH(state) ≠ 2`                                         | `non_standard_state_code`      |
| Invalid ZIP format          | `NOT REGEXP_CONTAINS(zip, r'^\d{5}(-\d{4})?$')`             | `invalid_zip_format`           |
| Future-dated order          | `order_date > CURRENT_DATE()`                               | `future_dated_order`           |

**Storage of Failed Rows:**

- Raw row preserved as JSON: `TO_JSON(t)` captures entire CSV row for forensics
- `failure_reasons` array: enables root-cause analysis and replay logic
- Example query to inspect dead letters:
  ```sql
  SELECT reason, COUNT(*) cnt
  FROM silver.orders_dead_letter, UNNEST(failure_reasons) reason
  WHERE load_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY reason ORDER BY cnt DESC;
  ```

---

### 3. Mart Layer

**Procedure:** `marts.sp_refresh_marts()`

Called by the gcs_loader function immediately after DQ checks pass.

#### Fact Table: `fct_daily_revenue`

```sql
SELECT
  order_date,
  store_name,
  category,
  COUNT(DISTINCT order_id)    AS orders,
  SUM(quantity)               AS units,
  SUM(revenue)                AS revenue,
  COUNT(DISTINCT customer_id) AS unique_customers
FROM silver.orders
GROUP BY order_date, store_name, category
```

**Grain:** Day + Store + Category  
**Indexing:**

- `PARTITION BY order_date` — partition elimination for time-series queries
- `CLUSTER BY store_name` — accelerates "get revenue by store" queries

**Refresh Strategy:** Full truncate + rebuild (cheap at this scale)

**Use Cases:**

- "Which store had the highest revenue yesterday?"
- "Compare yesterday to last week, by category"
- "Which category has the best margin?"

#### Dimension: `dim_product`

```sql
SELECT
  product_name,
  ANY_VALUE(category) AS category,
  SUM(quantity)       AS total_units_sold,
  SUM(revenue)        AS total_revenue,
  MIN(order_date)     AS first_sold_date,
  MAX(order_date)     AS last_sold_date
FROM silver.orders
GROUP BY product_name
```

**Grain:** Product  
**Temporal Tracking:**

- `first_sold_date`: when this product first appeared in orders
- `last_sold_date`: most recent order containing this product

**Use Cases:**

- "What are the top 10 best-selling products by revenue?"
- "Which products are trending (recently increasing)?
- "Which products haven't sold recently?"

#### Dimension: `dim_store`

```sql
SELECT
  store_name,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(revenue)             AS total_revenue,
  MIN(order_date)          AS first_order_date,
  MAX(order_date)          AS last_order_date
FROM silver.orders
GROUP BY store_name
```

**Grain:** Store  
**Use Cases:**

- "Which stores are underperforming?"
- "What's the total lifetime revenue by store?"

#### View: `fct_orders`

```sql
CREATE OR REPLACE VIEW marts.fct_orders AS
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
FROM silver.orders
```

**Purpose:** Full-grain drill-down for detailed questions  
**Use Cases:**

- "Show me all orders from store X on date Y"
- "What did customer ABC order?"

---

### 4. Orchestration

**Orchestrator:** Cloud Function (`gcs_loader`)

**Trigger:** Eventarc (`google.cloud.storage.object.finalize`)

**Execution Sequence:**

```python
@functions_framework.cloud_event
def gcs_loader(cloud_event: CloudEvent):
    # 1. Parse bucket/filename
    data = cloud_event.get_json_body()
    bucket_name = data["bucket"]
    object_name = data["name"]  # e.g., "incoming/orders_20260517.csv"

    # 2. Validate filename matches pattern: orders_YYYYMMDD.csv
    if not matches_pattern(object_name):
        log.error(f"Invalid filename: {object_name}")
        return

    # 3. Extract load_date from filename
    load_date = parse_load_date(object_name)  # "2026-05-17"

    # 4. Federated INSERT from GCS → raw.orders_raw
    load_raw_table(bucket_name, object_name, load_date)

    # 5. Run DQ stored procedure
    run_dq_checks(load_date)

    # 6. Refresh marts
    refresh_marts()

    # 7. Archive processed file
    archive_file(bucket_name, object_name)

    log.info("Pipeline completed successfully")
```

**Error Handling:**

- If any step fails, the Cloud Function logs the error and exits (exception bubbles)
- Dead Letter Queue (silver.orders_dead_letter) captures rows with validation failures
- Manual intervention triggers replay via gcs_loader invocation on the same file

**Idempotency:** Replaying the same file (by re-uploading to `incoming/`) will:

1. Clear the raw partition for load_date
2. Clear the silver partitions for load_date
3. Re-run all DQ checks
4. Rebuild marts

---

### 5. Database Layer

**Project Structure:**

```
${PROJECT_ID}
├── raw              (1 dataset)
│   └── orders_raw   (1 table, partitioned by load_date)
├── silver           (1 dataset)
│   ├── orders       (typed, partitioned by order_date, clustered by store_name)
│   └── orders_dead_letter (partitioned by load_date)
└── marts            (1 dataset)
    ├── fct_daily_revenue (fact)
    ├── dim_product (dimension)
    ├── dim_store (dimension)
    ├── fct_orders (view)
    └── sp_refresh_marts (procedure)
```

**BigQuery Configuration:**

| Setting            | Value             | Rationale                           |
| ------------------ | ----------------- | ----------------------------------- |
| Location           | US (multi-region) | Cost efficiency, latency            |
| Partitioning       | Date-based        | Partition elimination, cost control |
| Retention (raw)    | 90 days           | Compliance window, cost             |
| Retention (silver) | Unbounded         | Historical analysis                 |
| Retention (marts)  | Unbounded         | Active analytics                    |
| Clustering         | store_name        | Query optimization                  |

**Estimated Costs (monthly, ~10k orders/day):**

- Storage: ~$15 (silver + marts, unbounded; raw auto-expires)
- Query: ~$20 (DQ, marts refresh, ad-hoc)
- **Total:** ~$35/month

---

### 6. SQL-Based Transformations

All transformations are **pure BigQuery SQL** (no Dataflow, no Spark).

**Key Functions:**

- `SAFE.PARSE_DATE()` — type casting with NULL fallback
- `SAFE_CAST()` — integer/numeric casting
- `REGEXP_CONTAINS()` — pattern validation
- `ROW_NUMBER()` — deduplication
- `ARRAY_CONCAT()` — combining failure reasons

**Stored Procedures:**

- `sp_run_dq_checks(load_date)` — single partition replay
- `sp_refresh_marts()` — full rebuild (called after every successful load and hourly via scheduled query)

**Advantages:**

- No external dependencies (no Dataflow infrastructure overhead)
- Faster iteration (modify SQL, redeploy Cloud Function)
- Cheaper than Dataflow (BigQuery compute is per-query, not per-instance)
- Easier debugging (standard SQL, no Spark logs)

---

### 7. Data Quality & Error Handling

**DQ Framework:**

1. **Blocking Checks:** Prevent rows from reaching analytics
   - Rows failing any blocking check → `silver.orders_dead_letter`
   - Entire row stored as JSON for forensics
   - Failure reasons array for root-cause analysis

2. **Non-Blocking Flags:** Signal data quality issues but allow row through
   - Examples: revenue mismatch, invalid email, future-dated order
   - Stored in `dq_flags` array on the row in `silver.orders`
   - Enables analysts to filter/exclude flagged rows if needed

3. **Dead Letter Recovery:**

   ```sql
   -- 1. Inspect dead letters
   SELECT raw_row, failure_reasons
   FROM silver.orders_dead_letter
   WHERE load_date = '2026-05-17'
   LIMIT 10;

   -- 2. Fix at source (email attachment)
   -- 3. Re-upload: gcs_loader will clear partition and replay
   ```

4. **Monitoring Dashboard (Ad-Hoc):**
   ```sql
   SELECT
     load_date,
     reason,
     COUNT(*) AS count_rejected
   FROM silver.orders_dead_letter, UNNEST(failure_reasons) reason
   WHERE load_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
   GROUP BY load_date, reason
   ORDER BY load_date DESC, count_rejected DESC;
   ```

---

### 8. Pipeline Monitoring & Audit

**Audit Columns:**

Every table includes:

```sql
source_file    STRING   -- GCS path: gs://bucket/incoming/orders_YYYYMMDD.csv
load_date      DATE     -- Ingestion date (partition key)
loaded_at      TIMESTAMP-- Insertion timestamp
```

**Query to Audit Loads:**

```sql
SELECT
  load_date,
  COUNT(DISTINCT source_file) AS files_loaded,
  COUNT(*) AS total_rows,
  MIN(loaded_at) AS first_load_time,
  MAX(loaded_at) AS last_load_time
FROM silver.orders
WHERE load_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY load_date
ORDER BY load_date DESC;
```

**Apps Script Monitoring:**

Apps Script includes two triggers:

- **Time trigger (9-10 AM, every 5 min):** Poll for email and upload if found
- **Time trigger (10:00 AM):** Check if today's email was processed; if not, send admin alert

**Cloud Function Logs:**

- Stored in Cloud Logging
- Key log events:
  - File validated
  - Raw load completed (row count)
  - DQ checks completed (accepted/rejected counts)
  - Marts refresh completed
  - Archive completed

---

## Design Principles

### 1. **Minimize Complexity**

- Use native BigQuery SQL instead of external frameworks
- Avoid microservices unless justified (no Composer, no Scheduler)
- Single purpose per component (Apps Script → email, gcs_loader → pipeline)

### 2. **Idempotency**

- Replaying a file produces identical output
- Partition-level DELETE + INSERT ensures no duplicates
- Safe to trigger gcs_loader multiple times on the same file

### 3. **Data Quality First**

- DQ checks run **before** analytics
- Blocking failures never reach marts
- Non-blocking flags give analysts visibility into quality issues

### 4. **Cost Efficiency**

- Partition elimination minimizes scan scope
- Clustering on `store_name` optimizes chatbot queries (common drill-down dimension)
- Raw data expires after 90 days (staging cost is temporary)

### 5. **Auditability**

- Every row includes `source_file`, `load_date`, `loaded_at`
- Dead letter preserves entire raw row as JSON
- Failure reasons are explicit and searchable

### 6. **Operational Simplicity**

- No external schedulers; triggers are event-driven
- Failures surface in Cloud Function logs and Eventarc console
- Manual recovery is a re-upload to GCS (no complex replay logic)

### 7. **Scalability Constraints (Future-Ready)**

- Current design handles ~10k–100k orders/day comfortably
- At 1M+/day, evaluate: Dataflow for streaming, BigQuery Reservations for cost control
- Schema supports incremental date partitioning and new mart dimensions

---

## Assumptions

### Data Assumptions

1. **CSV Schema is Fixed:**
   - Column order and names match `EXPECTED_COLUMNS` in [functions/gcs_loader/schemas/orders.py](functions/gcs_loader/schemas/orders.py)
   - New columns require schema migration (terraform + DQ update)

2. **Load Date is in Filename:**
   - File named `orders_YYYYMMDD.csv` (e.g., `orders_20260517.csv`)
   - If filename is missing/invalid, current date is used as load_date

3. **Order ID is a Unique Business Key:**
   - Per business logic, `order_id` uniquely identifies a transaction
   - Duplicates across days are rejected; duplicates within a day are flagged

4. **Product Names are Naturally Clustered:**
   - Case and whitespace variations collapse after normalization
   - Same product won't have multiple category assignments (or rarely)

5. **Order Quantities and Prices are Non-Negative:**
   - Refunds/cancellations are not in this dataset
   - All `quantity ≥ 1`, `unit_price ≥ 0`, `revenue ≥ 0`

### Infrastructure Assumptions

6. **GCP Project Exists:**
   - BigQuery datasets and tables can be created
   - Service accounts have required IAM permissions
   - GCS bucket exists and is accessible

7. **Email is Received Daily:**
   - Apps Script assumes email arrives between 9-10 AM UTC (configurable in trigger)
   - If email is missed, the 10 AM alert triggers (admin manual retry)

8. **Cloud Function Can Access Raw/Silver/Marts Datasets:**
   - `gcs_loader` service account has `roles/bigquery.dataEditor` on datasets
   - `gcs_loader` service account has `roles/storage.objectAdmin` on landing bucket

9. **Chatbot is Only Consumer of Marts:**
   - No external BI tools directly query raw or silver layers
   - Future BI tool integrations should be planned (performance implications)

10. **Audit Requirements Are Met by Partition/Cluster Strategy:**
    - Date-based partitioning provides 90-day retention window for raw data
    - No GDPR/PII deletion logic implemented (scope for future phase)

### Operational Assumptions

11. **Manual Recovery is Acceptable:**
    - Failed loads are fixed at source and re-uploaded (no automatic retry)
    - Dead letter inspection and replay is manual but straightforward

12. **Marts Refresh Latency of 5–10 Minutes is Acceptable:**
    - Chatbot queries will see data ~5–10 min after ingestion
    - For sub-minute latency, requires streaming architecture (Pub/Sub → Dataflow)

13. **No Real-Time Streaming:**
    - Data arrives once daily via email
    - Batch processing (daily ingestion) is sufficient

14. **Scale is Manageable:**
    - Current design assumes < 100k orders/day
    - At 1M+/day, reevaluate: federated query performance, stored procedure limits

---

## Implementation Details

### Deployment

**Infrastructure as Code:** Terraform manages 20 resources:

- GCP APIs (BigQuery, Cloud Functions, Cloud Run, Eventarc, etc.)
- Service accounts and IAM bindings
- GCS buckets and lifecycle policies
- Cloud Functions (gcs_loader with code deployment)
- Cloud Run service (chatbot)
- BigQuery datasets, tables, and stored procedures

**SQL Schema Deployment:**

```bash
# Apply DDL for raw, silver, marts datasets
bq query --use_legacy_sql=false < sql/raw/01_orders_raw.sql
bq query --use_legacy_sql=false < sql/silver/01_orders_silver.sql
bq query --use_legacy_sql=false < sql/dq/01_sp_run_dq_checks.sql
bq query --use_legacy_sql=false < sql/marts/01_marts.sql
```

### Monitoring & Alerting

**Cloud Logging Queries:**

```
resource.type="cloud_function"
resource.labels.function_name="gcs_loader"
severity >= ERROR
```

**Apps Script Monitoring:**

- Time-based triggers are visible in Apps Script Dashboard
- Execution logs show success/failure for each 5-minute poll
- 10 AM alert email (via `sendEmail()`) if no today's file found

**BigQuery Monitoring:**

- Query performance in BigQuery console
- Scheduled query logs for marts refresh (hourly)
- Dead letter row count as proxy for DQ health

---

## Operational Considerations

### Maintenance

1. **DQ Rule Changes:**
   - Update `sp_run_dq_checks` SQL
   - Redeploy to BigQuery
   - Optional: replay historical data if needed

2. **Schema Evolution:**
   - Add column to raw/silver/marts DDL
   - Update `gcs_loader` schema validation
   - Redeploy Cloud Function

3. **Mart Rebuilds:**
   - Manual: `CALL marts.sp_refresh_marts();`
   - Scheduled: Hourly via BigQuery scheduled query
   - After any DQ rule change, manually rebuild once to flush old data

### Troubleshooting

| Issue                      | Root Cause                    | Resolution                                            |
| -------------------------- | ----------------------------- | ----------------------------------------------------- |
| No file in GCS             | Email not received            | Check inbox, resend manually to test account          |
| Raw table empty            | Cloud Function didn't trigger | Check Eventarc console, Cloud Function logs           |
| All rows in dead letter    | Data quality issue at source  | Inspect dead letter, fix CSV, re-upload               |
| Marts not updating         | sp_refresh_marts failed       | Check BigQuery job history, review procedure logs     |
| Chatbot returns no results | Empty marts                   | Verify silver.orders has rows, manually rebuild marts |

### Disaster Recovery

**Lost Raw Data (< 90 days):**

- Re-upload CSV from email to GCS incoming/
- gcs_loader will replay from cloud storage

**Lost Silver/Marts:**

- Replay from raw: `CALL silver.sp_run_dq_checks('2026-05-17');`
- Rebuild marts: `CALL marts.sp_refresh_marts();`

**Lost Raw (> 90 days):**

- Restore from email archive or original source system
- Re-upload CSV

---

## Conclusion

This Technical Design Document specifies the end-to-end architecture for automated e-commerce order ingestion, validation, and self-service analytics on GCP. The design prioritizes simplicity, cost efficiency, and operational reliability through a minimal set of managed services (Apps Script, GCS, Cloud Functions, BigQuery, Cloud Run) and pure SQL transformations.

**Key Success Criteria:**

- ✅ Fully automated daily ingestion (no manual download)
- ✅ Comprehensive DQ with blocking + non-blocking checks
- ✅ Self-serve AI chatbot for marketing questions
- ✅ Auditable pipeline with row-level traceability
- ✅ Sub-$50/month infrastructure cost
- ✅ 99% uptime SLA
