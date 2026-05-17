# Analysis Prompts — E‑Commerce Sales & Data Quality

Purpose: curated, reusable prompts for product, category, regional, and data quality analyses. Copy and adapt the prompt templates below in the AI agent powered by ADK.

## How to use

- Replace placeholders like `{{start_date}}`, `{{end_date}}`, `{{category}}`, `{{state}}`, `{{n}}`.
- For reproducible results, include time range and aggregation grain (day/week/month).
- Tag prompts by objective: performance, regional, customer, quality, strategy.

---

## Sales Performance

1. Which products generated the highest total revenue over {{start_date}} to {{end_date}}? Compare across categories and show top {{n}}.
2. Which categories have the highest average order value (AOV) and which have the highest total quantity sold in the period?
3. Which products have the highest revenue per order versus highest quantity sold? What does that imply about pricing and demand?

## Regional & State-Level Trends

4. What are the top-selling products by shipping state for {{start_date}}–{{end_date}}? Highlight regional buying patterns.
5. Are there any states where specific products consistently underperform or overperform in revenue? Show statistical significance if possible.
6. Which products are increasing or decreasing in sales velocity by region (week-over-week or month-over-month)?

## Customer & Order Insights

7. Which customers place the largest bulk orders? List top customers by total quantity and total spend; show most-frequently purchased products for each.
8. Can you identify clusters of customers (by region or behavior) that buy similar product sets suitable for targeted promotions?

## Product Strategy & Promotions

9. Which products or categories should be prioritized for promotions based on recent sales velocity and revenue contribution?
10. Are there categories where low-priced products drive high volume but lower profitability compared to premium items? Recommend focus areas.
11. Can you identify product bundling opportunities (products frequently ordered together), preferably by region?

## Prompt Templates / Examples

- Top N products by revenue:
  > "Show the top {{n}} products by total revenue between {{start_date}} and {{end_date}}, including category and total orders."
- State performance for a product:
  > "For product '{{product_name}}', list shipping states ordered by revenue and show month-over-month change."
- Data quality report:
  > "Scan product_name and shipping_state for anomalies: duplicates, typos, nulls, and uncommon values. Return counts and examples."
