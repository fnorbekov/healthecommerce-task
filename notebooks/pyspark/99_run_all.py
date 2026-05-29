# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Run the full medallion pipeline (one click)
# MAGIC
# MAGIC Convenience notebook: lands the raw file and runs Bronze -> Silver -> Gold
# MAGIC end-to-end via `src.pipeline.run_pipeline`. Use **Run all**.
# MAGIC
# MAGIC Re-run it as many times as you like — every write is idempotent, so the
# MAGIC final tables never gain duplicate rows.

# COMMAND ----------

import os
import shutil
import sys

repo_root = os.getcwd()
while repo_root != os.path.dirname(repo_root):
    if os.path.isdir(os.path.join(repo_root, "src")):
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)
        break
    repo_root = os.path.dirname(repo_root)

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

# Create schema + volume and land the sample data.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_fqn}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema}.{cfg.volume}")
shutil.copy(
    os.path.join(repo_root, "data", "raw", cfg.raw_filename), cfg.raw_volume_file
)
print(f"Landed -> {cfg.raw_volume_file}")

# COMMAND ----------

from src.pipeline import run_pipeline

metrics = run_pipeline(spark, cfg, cfg.raw_volume_file)
metrics

# COMMAND ----------

# MAGIC %md ### Gold · Total Revenue per Category

# COMMAND ----------

display(spark.table(cfg.fqn(cfg.gold_revenue_by_category)))

# COMMAND ----------

# MAGIC %md ### Gold · Total Units Sold per Region

# COMMAND ----------

display(spark.table(cfg.fqn(cfg.gold_units_by_region)))

# COMMAND ----------

# MAGIC %md ### Error storage (quarantined rows)

# COMMAND ----------

display(
    spark.table(cfg.fqn(cfg.errors_table)).select(
        "transaction_id", "amount", "error_reason"
    )
)
