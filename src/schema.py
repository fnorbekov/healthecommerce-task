"""Canonical schemas for the pipeline.

The raw layer is read with every field typed as STRING. This is deliberate:
raw e-commerce feeds are messy (negative amounts, blank ids, mixed date
formats). Reading as STRING guarantees ingestion never fails on a bad row;
type enforcement and the logic gate happen downstream in Silver.
"""
from pyspark.sql.types import StringType, StructField, StructType

# Raw landing schema — everything is a string on purpose (schema-on-read).
RAW_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("category", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

# Columns carried through to the cleansed Silver layer (after typing).
SILVER_COLUMNS = [
    "transaction_id",
    "event_timestamp",
    "event_date",
    "customer_id",
    "region",
    "amount",
    "quantity",
    "category",
    "status",
    "processed_at",
]
