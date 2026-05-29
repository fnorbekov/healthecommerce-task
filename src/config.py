"""Central configuration for the Health E-Commerce ETL pipeline.

All names are overridable via environment variables so the same code runs
unchanged on Databricks (Unity Catalog, 3-level names) and locally
(2-level names against the local Spark catalog).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    # --- Target namespace -------------------------------------------------
    # On Databricks Free Edition the default Unity Catalog is "workspace".
    # Locally, set HEC_CATALOG="" (empty) to use 2-level <schema>.<table> names.
    catalog: str = os.getenv("HEC_CATALOG", "workspace")
    schema: str = os.getenv("HEC_SCHEMA", "health_ecommerce")
    volume: str = os.getenv("HEC_VOLUME", "raw")

    # --- Source data ------------------------------------------------------
    raw_format: str = os.getenv("HEC_RAW_FORMAT", "json")  # json | csv
    raw_filename: str = os.getenv("HEC_RAW_FILENAME", "transactions.json")

    # --- Logical table names (medallion layers) ---------------------------
    bronze_table: str = "bronze_transactions"
    silver_table: str = "silver_transactions"
    errors_table: str = "errors_transactions"
    gold_revenue_by_category: str = "gold_revenue_by_category"
    gold_units_by_region: str = "gold_units_by_region"

    def fqn(self, table: str) -> str:
        """Fully-qualified table name. Drops the catalog when it is empty."""
        parts = [p for p in (self.catalog, self.schema, table) if p]
        return ".".join(parts)

    @property
    def schema_fqn(self) -> str:
        parts = [p for p in (self.catalog, self.schema) if p]
        return ".".join(parts)

    @property
    def volume_path(self) -> str:
        """FUSE path of the Unity Catalog volume that holds the raw landing file."""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def raw_volume_file(self) -> str:
        return f"{self.volume_path}/{self.raw_filename}"
