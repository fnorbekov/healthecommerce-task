"""Local entry point — run the full medallion pipeline off-platform.

    HEC_CATALOG="" python main.py

Setting HEC_CATALOG to empty uses 2-level <schema>.<table> names against the
local Spark catalog (no Unity Catalog required). On Databricks, use the
notebooks in notebooks/pyspark/ instead.
"""
from __future__ import annotations

import os

from src.config import Config
from src.pipeline import run_pipeline
from src.spark_session import get_spark


def main() -> None:
    # Default to local-friendly settings unless the caller overrides them.
    os.environ.setdefault("HEC_CATALOG", "")
    cfg = Config()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    source_path = os.path.join("data", "raw", cfg.raw_filename)
    metrics = run_pipeline(spark, cfg, source_path)

    print("\n=== Pipeline metrics ===")
    for key, value in metrics.items():
        print(f"  {key:>20}: {value}")

    print("\n=== Gold · revenue by category ===")
    spark.table(cfg.fqn(cfg.gold_revenue_by_category)).show(truncate=False)

    print("=== Gold · units by region ===")
    spark.table(cfg.fqn(cfg.gold_units_by_region)).show(truncate=False)

    print("=== Errors (quarantined) ===")
    spark.table(cfg.fqn(cfg.errors_table)).select(
        "transaction_id", "amount", "error_reason"
    ).show(truncate=False)


if __name__ == "__main__":
    main()
