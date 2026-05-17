"""
Stakeholder chatbot — Google ADK agent with BigQuery toolset.

Marketing manager asks "how did we do last month?" in plain English.
Agent: Gemini 2.5 (via Vertex AI) with BigQueryToolset.
The toolset exposes parameterized SQL execution to the agent — no SQL injection,
the agent cannot drop tables or query outside the allowlisted dataset.

Dataset is restricted to `marts` — fast, narrow, no PII. silver/raw are not exposed.
"""

from __future__ import annotations

import os

from google.adk.agents import Agent
from google.adk.tools.bigquery import BigQueryCredentialsConfig, BigQueryToolset
from google.adk.tools.bigquery.config import BigQueryToolConfig, WriteMode
import google.auth

PROJECT_ID = os.environ["GCP_PROJECT"]
LOCATION   = os.environ.get("BQ_LOCATION", "US")
DATASET    = os.environ.get("MARTS_DATASET", "marts")

# Agent runs with Application Default Credentials (the Cloud Run service account).
creds, _ = google.auth.default()
credentials_config = BigQueryCredentialsConfig(credentials=creds)

# Read-only. The agent can SELECT, never INSERT/UPDATE/DELETE/CREATE.
tool_config = BigQueryToolConfig(write_mode=WriteMode.BLOCKED)

bq_toolset = BigQueryToolset(
    credentials_config=credentials_config,
    bigquery_tool_config=tool_config,
)

INSTRUCTION = f"""\
You are a friendly business analyst for an e-commerce company. You answer questions about
sales, products, stores, customers, and revenue using BigQuery.

# Rules
- Only query the `{PROJECT_ID}.{DATASET}` dataset. Available tables:
    * fct_daily_revenue (order_date, store_name, category, orders, units, revenue, unique_customers)
    * dim_product       (product_name, category, total_units_sold, total_revenue, ...)
    * dim_store         (store_name, total_orders, total_revenue, first_order_date, last_order_date)
    * fct_orders        (order grain — use only for drill-down questions)
- Always include date filters when possible (data is partitioned by date — saves cost).
- Prefer the rollup tables (fct_daily_revenue, dim_*) over fct_orders.
- When the user says "last month", interpret as the previous calendar month relative to today.
- When the user says "best" or "top", default to top 5 by revenue and ASK if they meant something else.
- Format money as USD with thousands separators. Format percentages with 1 decimal.
- If the answer is a list, show a small markdown table.
- If a question is ambiguous (e.g. "performance" — by revenue? orders? units?), ask one clarifying
  question before querying.
- Never invent numbers. If a query returns nothing, say so.

# Style
- Concise. Lead with the answer, then the supporting numbers.
- No SQL in the response unless the user asks for it.
"""

root_agent = Agent(
    name="ecom_bi_agent",
    model="gemini-2.5-flash",
    description="Answers business questions about e-commerce sales using BigQuery marts.",
    instruction=INSTRUCTION,
    tools=[bq_toolset],
)
