"""Delta write helpers that guarantee idempotency.

Requirement #4: running the pipeline multiple times on the same input must not
create duplicate records in the final tables.

Two strategies are used:
  * overwrite_table  — full recompute; the table is replaced wholesale. Used for
                       Bronze, the Errors table and Gold summaries. Trivially
                       idempotent.
  * merge_upsert     — Delta MERGE on a business key. Used for Silver, where we
                       want incremental upserts without duplicating existing
                       transactions on re-run.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def ensure_namespace(spark: SparkSession, cfg) -> None:
    """Create the catalog schema (and assume the catalog already exists)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_fqn}")


def overwrite_table(df: DataFrame, table_fqn: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_fqn)
    )


def merge_upsert(spark: SparkSession, df: DataFrame, table_fqn: str, key: str) -> None:
    """Upsert ``df`` into ``table_fqn`` matching on ``key`` (creates if absent)."""
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(table_fqn):
        # Create an empty table with the right schema, then merge into it.
        df.limit(0).write.format("delta").saveAsTable(table_fqn)

    target = DeltaTable.forName(spark, table_fqn)
    (
        target.alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
