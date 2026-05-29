"""Silver transforms — standardize, cast, deduplicate.

Requirement #2:
  * Standardize date/time formats  -> single TimestampType column (UTC-normalized)
  * Handle duplicates              -> keep one row per transaction_id (latest)
  * Cast fields to correct types   -> int / double / timestamp / string
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .schema import SILVER_COLUMNS


def standardize_and_cast(df: DataFrame) -> DataFrame:
    """Cast raw string fields to their proper types and normalize timestamps.

    ISO8601 inputs arrive in several shapes (``...Z``, ``+00:00``, ``-05:00``,
    with/without milliseconds). ``to_timestamp`` is attempted with a set of
    explicit patterns and coalesced, so all variants normalize to one
    TimestampType column.
    """
    ts_raw = F.col("timestamp")
    event_ts = F.coalesce(
        F.to_timestamp(ts_raw),  # Spark's built-in ISO parser (handles Z / offset)
        F.to_timestamp(ts_raw, "yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX"),
        F.to_timestamp(ts_raw, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(ts_raw, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        F.to_timestamp(ts_raw, "yyyy-MM-dd HH:mm:ss"),
    )

    return (
        df.withColumn("event_timestamp", event_ts)
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("amount", F.round(F.col("amount").cast("double"), 2))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("region", F.trim(F.col("region")))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("processed_at", F.current_timestamp())
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep a single row per transaction_id (the most recent by event_timestamp)."""
    window = Window.partitionBy("transaction_id").orderBy(
        F.col("event_timestamp").desc_nulls_last(),
        F.col("processed_at").desc(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def to_silver(df: DataFrame) -> DataFrame:
    """Full Silver transform: standardize + cast + deduplicate -> final columns."""
    typed = standardize_and_cast(df)
    deduped = deduplicate(typed)
    return deduped.select(*SILVER_COLUMNS)
