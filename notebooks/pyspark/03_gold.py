# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — curated business summaries
# MAGIC * Total Revenue per Category
# MAGIC * Total Units Sold per Region
# MAGIC
# MAGIC Both tables are fully recomputed from Silver and **overwritten**, so they
# MAGIC are idempotent by construction.

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

from src.aggregation import revenue_by_category, units_by_region
from src.io_utils import overwrite_table

silver = spark.table(cfg.fqn(cfg.silver_table))

overwrite_table(revenue_by_category(silver), cfg.fqn(cfg.gold_revenue_by_category))
overwrite_table(units_by_region(silver), cfg.fqn(cfg.gold_units_by_region))

# COMMAND ----------

# MAGIC %md ### Total Revenue per Category

# COMMAND ----------

display(spark.table(cfg.fqn(cfg.gold_revenue_by_category)))

# COMMAND ----------

# MAGIC %md ### Total Units Sold per Region

# COMMAND ----------

display(spark.table(cfg.fqn(cfg.gold_units_by_region)))
