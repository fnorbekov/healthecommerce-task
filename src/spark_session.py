"""Spark session factory.

On Databricks a session already exists and is returned as-is. Locally we build
a Delta-enabled session so the exact same pipeline code can be run and tested
off-platform.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "hec-etl") -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        # Running on Databricks (or an already-configured session).
        return active

    # Local fallback: configure Delta Lake explicitly.
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        .config("spark.sql.shuffle.partitions", "4")
    )
    try:
        from delta import configure_spark_with_delta_pip

        return configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception:
        # delta-spark not installed; return a plain session (parquet fallback).
        return builder.getOrCreate()
