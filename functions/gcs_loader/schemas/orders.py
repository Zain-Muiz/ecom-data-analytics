from pydantic import BaseModel
from google.cloud import bigquery


class OrdersRawSchema(BaseModel):
    order_id: str
    order_date: str
    customer_id: str
    customer_email: str
    product_name: str
    category: str
    quantity: str
    unit_price: str
    revenue: str
    store_name: str
    shipping_city: str
    shipping_state: str
    shipping_zip: str
    source_file: str
    load_date: str
    loaded_at: str


EXPECTED_COLUMNS = list(OrdersRawSchema.model_fields.keys())


BQ_SCHEMA = [
    bigquery.SchemaField("order_id", "STRING"),
    bigquery.SchemaField("order_date", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("customer_email", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("quantity", "STRING"),
    bigquery.SchemaField("unit_price", "STRING"),
    bigquery.SchemaField("revenue", "STRING"),
    bigquery.SchemaField("store_name", "STRING"),
    bigquery.SchemaField("shipping_city", "STRING"),
    bigquery.SchemaField("shipping_state", "STRING"),
    bigquery.SchemaField("shipping_zip", "STRING"),
    bigquery.SchemaField("source_file", "STRING"),
    bigquery.SchemaField("load_date", "DATE"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]
