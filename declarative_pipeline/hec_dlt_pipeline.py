"""Health E-Commerce — Lakeflow Declarative Pipeline (DLT).

Same medallion architecture as the PySpark-notebook approach, expressed
declaratively. Lakeflow figures out the dependency graph (bronze -> silver ->
gold) and orchestrates the run.

Why this is idempotent (Requirement #4)
----------------------------------------
Every dataset below is a **materialized view** (``@dlt.table`` over a batch
read). On each pipeline update the engine recomputes the view from its inputs
and atomically replaces the table contents — there is no append path, so
re-running can never produce duplicate rows in the final tables.

Error handling (Requirement #1)
-------------------------------
Two complementary mechanisms:
  * ``errors_transactions`` — an explicit "Error storage" table holding every
    record with a missing transaction_id or a negative amount, plus a reason.
  * ``@dlt.expect_or_drop`` expectations on Silver — the same bad records are
    dropped from the clean path and counted in the pipeline's data-quality
    metrics. The pipeline still completes successfully.

Configuration
-------------
Reads the raw landing path from the pipeline configuration key ``hec.raw_path``
(falls back to the default Volume path). Set the pipeline's target catalog and
schema in the pipeline settings UI (recommended: schema ``health_ecommerce_dlt``
so it does not collide with the notebook approach's tables).
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_PATH = spark.conf.get(
    "hec.raw_path", "/Volumes/workspace/health_ecommerce/raw/transactions.json"
)

# Schema-on-read: every field is a string at the raw layer.
RAW_COLUMNS = [
    "transaction_id",
    "timestamp",
    "customer_id",
    "region",
    "amount",
    "quantity",
    "category",
    "status",
]
RAW_SCHEMA = ", ".join(f"{c} STRING" for c in RAW_COLUMNS)

# Reusable data-quality predicates (SQL strings, used by the errors table and
# by the Silver expectations so the two stay in lock-step).
RULE_VALID_ID = "transaction_id IS NOT NULL AND trim(transaction_id) <> ''"
RULE_NON_NEGATIVE = "amount_dbl IS NULL OR amount_dbl >= 0"


# ---------------------------------------------------------------------------
# Bronze — raw ingestion (snapshot)
# ---------------------------------------------------------------------------
@dlt.table(
    name="bronze_transactions",
    comment="Raw transactions exactly as received (schema-on-read, all strings).",
    table_properties={"quality": "bronze"},
)
def bronze_transactions():
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("multiLine", True)
        .json(RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Error storage — quarantined records (missing id OR negative amount)
# ---------------------------------------------------------------------------
@dlt.table(
    name="errors_transactions",
    comment="Records rejected by the logic gate, with the reason for rejection.",
    table_properties={"quality": "quarantine"},
)
def errors_transactions():
    amt = F.col("amount").cast("double")
    missing_id = F.col("transaction_id").isNull() | (
        F.trim(F.col("transaction_id")) == F.lit("")
    )
    negative_amount = F.coalesce(amt < F.lit(0), F.lit(False))
    reason = F.concat_ws(
        "; ",
        F.when(missing_id, F.lit("missing_transaction_id")),
        F.when(negative_amount, F.lit("negative_amount")),
    )
    return (
        dlt.read("bronze_transactions")
        .filter(missing_id | negative_amount)
        .withColumn("error_reason", reason)
        .withColumn("quarantined_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Silver — cleansed, type-cast, deduplicated (bad rows dropped via expectations)
# ---------------------------------------------------------------------------
@dlt.table(
    name="silver_transactions",
    comment="Cleansed, typed, de-duplicated transactions ready for analytics.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_transaction_id", RULE_VALID_ID)
@dlt.expect_or_drop("non_negative_amount", RULE_NON_NEGATIVE)
def silver_transactions():
    bronze = dlt.read("bronze_transactions")

    # Standardize the ISO8601 timestamp (handles Z / +00:00 / -05:00 / millis).
    ts_raw = F.col("timestamp")
    event_ts = F.coalesce(
        F.to_timestamp(ts_raw),
        F.to_timestamp(ts_raw, "yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX"),
        F.to_timestamp(ts_raw, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(ts_raw, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )

    typed = (
        bronze.withColumn("amount_dbl", F.round(F.col("amount").cast("double"), 2))
        .withColumn("event_timestamp", event_ts)
        .withColumn("event_date", F.to_date(event_ts))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("region", F.trim(F.col("region")))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
    )

    # Deduplicate: one row per transaction_id, keeping the latest event.
    window = Window.partitionBy("transaction_id").orderBy(
        F.col("event_timestamp").desc_nulls_last()
    )
    deduped = (
        typed.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return deduped.select(
        "transaction_id",
        "event_timestamp",
        "event_date",
        "customer_id",
        "region",
        F.col("amount_dbl").alias("amount"),
        "quantity",
        "category",
        "status",
    )


# ---------------------------------------------------------------------------
# Gold — curated business summaries
# ---------------------------------------------------------------------------
@dlt.table(
    name="gold_revenue_by_category",
    comment="Total revenue and transaction count per product category.",
    table_properties={"quality": "gold"},
)
def gold_revenue_by_category():
    return (
        dlt.read("silver_transactions")
        .groupBy("category")
        .agg(
            F.round(F.sum("amount"), 2).alias("total_revenue"),
            F.count("*").alias("transaction_count"),
        )
    )


@dlt.table(
    name="gold_units_by_region",
    comment="Total units sold and transaction count per region.",
    table_properties={"quality": "gold"},
)
def gold_units_by_region():
    return (
        dlt.read("silver_transactions")
        .groupBy("region")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.count("*").alias("transaction_count"),
        )
    )
