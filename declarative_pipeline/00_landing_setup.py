# Databricks notebook source
# MAGIC %md
# MAGIC # Declarative Pipeline · Landing setup
# MAGIC
# MAGIC A Lakeflow Declarative Pipeline only **defines datasets** — it cannot copy
# MAGIC files. Run this small notebook **once** to create the shared raw Volume and
# MAGIC land the sample dataset into it. The declarative pipeline then reads from
# MAGIC `/Volumes/<catalog>/health_ecommerce/raw/transactions.json`.
# MAGIC
# MAGIC > If you already ran `notebooks/pyspark/00_setup.py`, the Volume and file
# MAGIC > already exist and you can skip this.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "health_ecommerce")
dbutils.widgets.text("volume", "raw")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

# COMMAND ----------

import os
import shutil

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

repo_root = os.getcwd()
while repo_root != os.path.dirname(repo_root):
    if os.path.isdir(os.path.join(repo_root, "src")):
        break
    repo_root = os.path.dirname(repo_root)

dst = f"/Volumes/{catalog}/{schema}/{volume}/transactions.json"
shutil.copy(os.path.join(repo_root, "data", "raw", "transactions.json"), dst)
print(f"Raw file ready at: {dst}")
print("Set this as `hec.raw_path` in the pipeline configuration (or use the default).")
