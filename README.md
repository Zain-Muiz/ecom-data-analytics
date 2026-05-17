# E-commerce BI Platform on GCP

Solves three problems for the BI team:

1. **Automate ingestion** — Daily CSV from email lands in BigQuery without manual intervention.
2. **Catch bad data automatically** — Every row passes through SQL-based DQ; rejects go to a dead letter table.
3. **Self-serve answers** — Marketing asks plain-English questions, an ADK agent queries BigQuery and answers.

Built with the smallest set of services that does the job: **Apps Script + GCS + Cloud Function + BigQuery + Cloud Run + ADK with Vertex AI**. No Dataflow, no Composer, no Cloud Scheduler, no Secret Manager.

---

## Architecture

```
Daily email ──► Apps Script (in inbox owner's account)
                    │ time triggers: poll every 5min in 9-10 AM window
                    │                alert at 10 AM if missing
                    ▼
                GCS incoming/ ──► gcs_loader (CF on object.finalize)
                                            │
                                            ▼
                                    raw.orders_raw
                                            │
                                            ▼
                                    sp_run_dq_checks
                                  ┌─────────┴──────────┐
                                  ▼                    ▼
                              silver.orders     silver.orders_dead_letter
                              (with dq_flags)   (failure_reasons)
                                  │
                                  ▼
                            sp_refresh_marts (same loader call)
                                  │
                                  ▼
                          ADK Chatbot (Cloud Run) ◄── reads marts.*
```

See [`docs/architecture.md`](docs/architecture.md) for the full mermaid diagrams.

## Repo layout

```
.
├── apps_script/                 # Email-to-GCS — runs in inbox owner's Google account
│   ├── Code.gs
│   ├── appsscript.json          # OAuth scope manifest
│   └── README.md                # 5-step setup
├── functions/gcs_loader/        # Eventarc on object.finalize → load → DQ → refresh marts → archive
├── sql/
│   ├── raw/                     # raw.orders_raw DDL
│   ├── silver/                  # silver.orders + dead_letter DDL
│   ├── dq/                      # sp_run_dq_checks
│   └── marts/                   # marts tables + sp_refresh_marts
├── ecom-agent/                     # ADK agent + Dockerfile for Cloud Run
├── terraform/                   # 20 resources, all justified
├── scripts/                     # data generator, BQ DDL applier, backfill
└── docs/architecture.md
```

---

## Data quality

DQ is one BigQuery stored procedure: [`sql/dq/01_sp_run_dq_checks.sql`](sql/dq/01_sp_run_dq_checks.sql).

**Blocking checks** (row → `silver.orders_dead_letter` with `failure_reasons` array):

| Check                                         | Reason code                          |
| --------------------------------------------- | ------------------------------------ |
| `order_id` null/empty                         | `missing_order_id`                   |
| `order_date` invalid or null                  | `invalid_or_missing_order_date`      |
| `quantity` ≤ 0 or null                        | `invalid_quantity`                   |
| `unit_price` < 0 or null                      | `invalid_unit_price`                 |
| `revenue` < 0 or null                         | `invalid_revenue`                    |
| `store_name` missing                          | `missing_store_name`                 |
| `product_name` missing                        | `missing_product_name`               |
| Same `(order_id, product)` twice in same file | `duplicate_order_product_in_file`    |
| `order_id` already loaded in a previous file  | `duplicate_order_id_seen_previously` |

**Non-blocking flags** (row goes to silver, but `dq_flags` records the issue for monitoring):

| Check                                      | Flag                           |
| ------------------------------------------ | ------------------------------ |
| `revenue ≠ qty × unit_price` (>1c)         | `revenue_mismatch_qty_x_price` |
| Bad email format                           | `invalid_email_format`         |
| State code not 2 chars (e.g. "California") | `non_standard_state_code`      |
| Bad ZIP format                             | `invalid_zip_format`           |
| Order date in the future                   | `future_dated_order`           |

The procedure is **idempotent** — replay a day's load and the partition is rebuilt cleanly.

Product naming is normalized at the staging step (`INITCAP(TRIM(...))`) so "iPhone 14", "IPHONE 14", and " iphone 14 " collapse to the same product.

### Monitoring DQ

```sql
-- Top failure reasons last 7 days
SELECT reason, COUNT(*) cnt
FROM silver.orders_dead_letter, UNNEST(failure_reasons) reason
WHERE load_date >= CURRENT_DATE() - 7
GROUP BY reason ORDER BY cnt DESC;
```

---

## Deploy

```bash
export PROJECT_ID=ecom-bi-prod
gcloud config set project $PROJECT_ID

# 1. Terraform — provisions GCS, BQ datasets, loader function, IAM
cd terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars
terraform init && terraform apply

# 2. Apply BQ DDL
Apply All DDLs in ./sql

# 3. Set up Apps Script (in inbox owner's Google account)
#    Follow apps_script/README.md — paste Code.gs, set GCP project, install triggers

# 4. Grant the inbox owner write access to the landing bucket
gsutil iam ch user:data-ingest@yourcompany.com:objectCreator gs://ecom-bi-landing-prod

# 5. Generate test data and backfill
python scripts/generate_data.py --backfill --out ./generated
BUCKET=ecom-bi-landing-prod ./scripts/backfill.sh ./generated

# 6. Deploy chatbot to Cloud Run
gcloud run deploy ecom-bi-chatbot \
  --source ./chatbot \
  --region us-central1 \
  --service-account "chatbot-runner@${PROJECT_ID}.iam.gserviceaccount.com" \
  --set-env-vars "GCP_PROJECT=${PROJECT_ID},GOOGLE_GENAI_USE_VERTEXAI=TRUE,GOOGLE_CLOUD_LOCATION=us-central1"
```

---

## Cost estimate

| Component                   | Cost at spec scale                         |
| --------------------------- | ------------------------------------------ |
| Apps Script                 | Free (consumer Google account)             |
| GCS storage                 | $0                                         |
| Cloud Function (gcs_loader) | <$0.01/month                               |
| BigQuery storage + query    | <$0.05/month (under free tier for storage) |
| Cloud Run (chatbot)         | <$0.50/month (scales to zero)              |
| Vertex AI Gemini Flash      | ~$1/month at 100 chats                     |
| **Total**                   | **<$2/month**                              |

---

## What I'd do differently at higher scale

| Concern       | At spec scale         | At 100M+ rows/day                             |
| ------------- | --------------------- | --------------------------------------------- |
| Email→GCS     | Apps Script           | Google Workspace push notifications + Pub/Sub |
| Loader        | Cloud Function        | Dataflow Template                             |
| DQ            | BQ stored proc        | Same proc + Dataplex auto-DQ                  |
| Marts refresh | Synchronous in loader | Incremental dbt models / materialized views   |
| Chatbot       | Single ADK agent      | Multi-agent: NL2SQL + saved-query agents      |
| CI/CD         | `terraform apply`     | Cloud Build pipelines, plan-on-PR             |
