# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — raw ingestion
# MAGIC Reads the landed raw file (schema-on-read, all strings) and writes the
# MAGIC `bronze_transactions` Delta table. Bronze is an **overwrite** snapshot of
# MAGIC the source, so re-running reproduces it exactly (idempotent).

# COMMAND ----------

import os
import sys

# Put the repo root (folder containing `src/`) on the Python path.
_p = os.getcwd()
while _p != os.path.dirname(_p):
    if os.path.isdir(os.path.join(_p, "src")):
        if _p not in sys.path:
            sys.path.insert(0, _p)
        break
    _p = os.path.dirname(_p)

from src.config import Config

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "health_ecommerce")
dbutils.widgets.text("volume", "raw")
cfg = Config(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    volume=dbutils.widgets.get("volume"),
)

# COMMAND ----------

from src.ingestion import ingest_to_bronze

bronze = ingest_to_bronze(spark, cfg, cfg.raw_volume_file)
print(f"Bronze rows: {bronze.count()}  ->  {cfg.fqn(cfg.bronze_table)}")
display(bronze)
