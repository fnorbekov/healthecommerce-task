"""End-to-end orchestration of the medallion pipeline.

    raw file ->  Bronze (overwrite)
              ->  logic gate  -> Errors (overwrite)
                              -> Silver (standardize + cast + dedup, MERGE upsert)
              ->  Gold        -> revenue_by_category (overwrite)
                              -> units_by_region     (overwrite)

Every write is idempotent, so re-running on the same input is safe.
"""
from __future__ import annotations

from typing import Dict

from pyspark.sql import SparkSession

from .aggregation import revenue_by_category, units_by_region
from .config import Config
from .ingestion import ingest_to_bronze
from .io_utils import ensure_namespace, merge_upsert, overwrite_table
from .transformation import to_silver
from .validation import split_valid_invalid


def run_pipeline(spark: SparkSession, cfg: Config, source_path: str) -> Dict[str, int]:
    """Run Bronze -> Silver -> Gold and return row-count metrics."""
    ensure_namespace(spark, cfg)

    # --- Bronze: raw snapshot --------------------------------------------
    bronze = ingest_to_bronze(spark, cfg, source_path)

    # --- Logic gate: split valid vs quarantined --------------------------
    valid, errors = split_valid_invalid(bronze)
    overwrite_table(errors, cfg.fqn(cfg.errors_table))

    # --- Silver: cleansed, deduplicated, upserted ------------------------
    silver = to_silver(valid)
    merge_upsert(spark, silver, cfg.fqn(cfg.silver_table), key="transaction_id")
    silver_tbl = spark.table(cfg.fqn(cfg.silver_table))

    # --- Gold: business summaries ----------------------------------------
    overwrite_table(
        revenue_by_category(silver_tbl), cfg.fqn(cfg.gold_revenue_by_category)
    )
    overwrite_table(units_by_region(silver_tbl), cfg.fqn(cfg.gold_units_by_region))

    metrics = {
        "bronze_rows": bronze.count(),
        "error_rows": spark.table(cfg.fqn(cfg.errors_table)).count(),
        "silver_rows": silver_tbl.count(),
        "revenue_categories": spark.table(
            cfg.fqn(cfg.gold_revenue_by_category)
        ).count(),
        "regions": spark.table(cfg.fqn(cfg.gold_units_by_region)).count(),
    }
    return metrics
