"""Gold layer — curated business summaries.

Requirement #3:
  * Total Revenue per Category
  * Total Units Sold per Region
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def revenue_by_category(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("category")
        .agg(
            F.round(F.sum("amount"), 2).alias("total_revenue"),
            F.count("*").alias("transaction_count"),
        )
        .orderBy(F.col("total_revenue").desc())
    )


def units_by_region(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("region")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.count("*").alias("transaction_count"),
        )
        .orderBy(F.col("total_units_sold").desc())
    )
