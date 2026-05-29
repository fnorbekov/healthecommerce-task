# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — logic gate, cleanse, deduplicate
# MAGIC * **Logic gate** — rows with a missing `transaction_id` or a negative
# MAGIC   `amount` are routed to `errors_transactions` (overwrite, deterministic).
# MAGIC * **Cleanse** — standardize timestamps, cast types.
# MAGIC * **Deduplicate** — one row per `transaction_id` (latest wins).
# MAGIC * **Idempotent write** — Delta `MERGE` upsert on `transaction_id`.

# COMMAND ----------

import os
import sys

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

from src.io_utils import merge_upsert, overwrite_table
from src.transformation import to_silver
from src.validation import split_valid_invalid

bronze = spark.table(cfg.fqn(cfg.bronze_table))

# Logic gate -> quarantine the bad rows
valid, errors = split_valid_invalid(bronze)
overwrite_table(errors, cfg.fqn(cfg.errors_table))

# Cleanse + dedup -> idempotent upsert into Silver
silver = to_silver(valid)
merge_upsert(spark, silver, cfg.fqn(cfg.silver_table), key="transaction_id")

print(f"Errors quarantined : {spark.table(cfg.fqn(cfg.errors_table)).count()}")
print(f"Silver rows        : {spark.table(cfg.fqn(cfg.silver_table)).count()}")

# COMMAND ----------

# MAGIC %md ### Quarantined records (Error storage)

# COMMAND ----------

display(
    spark.table(cfg.fqn(cfg.errors_table)).select(
        "transaction_id", "amount", "error_reason", "quarantined_at"
    )
)

# COMMAND ----------

# MAGIC %md ### Cleansed Silver records

# COMMAND ----------

display(spark.table(cfg.fqn(cfg.silver_table)))
