"""Bronze layer — raw ingestion.

Reads the raw transaction file (schema-on-read, all strings) and lands it in a
Bronze Delta table. Bronze is a *snapshot* of the source, so it is written with
``overwrite``: re-running the pipeline on the same input reproduces the exact
same Bronze table (idempotent).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config
from .schema import RAW_SCHEMA


def read_raw(spark: SparkSession, source_path: str, fmt: str = "json") -> DataFrame:
    """Read the raw feed from a file path (Volume path on Databricks)."""
    reader = spark.read.schema(RAW_SCHEMA)
    if fmt == "json":
        # multiLine=True so a pretty-printed JSON array is parsed correctly.
        df = reader.option("multiLine", True).json(source_path)
    elif fmt == "csv":
        df = reader.option("header", True).csv(source_path)
    else:
        raise ValueError(f"Unsupported raw format: {fmt}")

    return df.withColumn("_source_file", F.lit(source_path)).withColumn(
        "_ingested_at", F.current_timestamp()
    )


def write_bronze(df: DataFrame, cfg: Config) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(cfg.fqn(cfg.bronze_table))
    )


def ingest_to_bronze(spark: SparkSession, cfg: Config, source_path: str) -> DataFrame:
    raw = read_raw(spark, source_path, cfg.raw_format)
    write_bronze(raw, cfg)
    return spark.table(cfg.fqn(cfg.bronze_table))
