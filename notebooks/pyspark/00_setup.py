# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — catalog, schema, volume & raw landing
# MAGIC
# MAGIC Run this **once** (or whenever you re-clone the repo). It:
# MAGIC 1. creates the target schema in Unity Catalog,
# MAGIC 2. creates a Volume to hold the raw landing file,
# MAGIC 3. copies the sample dataset from the Git folder into that Volume.
# MAGIC
# MAGIC Adjust the widgets at the top if your catalog is not `workspace`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "health_ecommerce")
dbutils.widgets.text("volume", "raw")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
print(f"Schema  : {catalog}.{schema}")
print(f"Volume  : /Volumes/{catalog}/{schema}/{volume}")

# COMMAND ----------

# Locate the repo root (the folder that contains `src/`) and copy the
# sample dataset into the Volume so Spark can read it reliably.
import os
import shutil

repo_root = os.getcwd()
while repo_root != os.path.dirname(repo_root):
    if os.path.isdir(os.path.join(repo_root, "src")):
        break
    repo_root = os.path.dirname(repo_root)

src_file = os.path.join(repo_root, "data", "raw", "transactions.json")
dst_file = f"/Volumes/{catalog}/{schema}/{volume}/transactions.json"
shutil.copy(src_file, dst_file)
print(f"Landed raw file -> {dst_file}")

# COMMAND ----------

# MAGIC %md ### Preview of the landed raw data (note the intentional bad rows)

# COMMAND ----------

display(spark.read.option("multiLine", True).json(dst_file))
