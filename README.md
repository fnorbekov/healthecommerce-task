# Health E-Commerce — Batch ETL Pipeline (Medallion)

A batch ETL pipeline that moves e-commerce transaction data from a **raw** state
into a curated **Gold** reporting layer for BI & Analytics, built for
**Databricks Free Edition** and version-controlled on GitHub.

The same medallion architecture is delivered **two ways**:

| # | Approach | Location | How you run it |
|---|----------|----------|----------------|
| 1 | **PySpark notebooks** (modular `src/` package) | [`notebooks/pyspark/`](notebooks/pyspark/) | Run the notebooks (or `99_run_all`) on serverless compute |
| 2 | **Declarative pipeline** (Lakeflow / DLT) | [`declarative_pipeline/`](declarative_pipeline/) | Create a Lakeflow pipeline pointing at the source file |

> Architecture details and diagram: [`docs/architecture.md`](docs/architecture.md).

---

## What the pipeline does (maps to the assessment requirements)

| Requirement | Where it's implemented |
|---|---|
| Sample data (30 rows: valid, missing id, negative amount, duplicates) | [`data/raw/transactions.json`](data/raw/transactions.json) |
| **1. Logic gate** → bad records to **Error storage**, pipeline still succeeds | [`src/validation.py`](src/validation.py) · `errors_transactions` table |
| **2. Cleanse** — standardize timestamps, dedup, cast types | [`src/transformation.py`](src/transformation.py) |
| **3. Gold summaries** — revenue per category, units per region | [`src/aggregation.py`](src/aggregation.py) |
| **4. Idempotent re-runs** (no duplicates in final tables) | [`src/io_utils.py`](src/io_utils.py) — overwrite + Delta `MERGE` |
| Modular Python (PySpark) | [`src/`](src/) |
| README + architecture diagram | this file + [`docs/architecture.md`](docs/architecture.md) |

### The sample dataset (intentional data issues)

- **20** valid standard transactions
- **2** duplicates — `T-1001`, `T-1005` appear twice (later timestamp wins)
- **2** missing `transaction_id` — one `null`, one blank `""`
- **2** negative `amount` — `T-1023`, `T-1024`
- Mixed ISO8601 timestamp formats (`...Z`, `+00:00`, `-05:00`, milliseconds) to
  exercise date standardization

Expected outcome: **4** rows quarantined to `errors_transactions`, **24** clean
rows deduplicated to **22** in `silver_transactions`.

---

## Repository layout

```
healthecommerce-task/
├── data/raw/transactions.json        # sample dataset (30 rows)
├── src/                              # shared, modular PySpark logic
│   ├── config.py                     # catalog/schema/table names (env-overridable)
│   ├── schema.py                     # raw + silver schemas
│   ├── spark_session.py              # Databricks-or-local Spark factory
│   ├── ingestion.py                  # Bronze
│   ├── validation.py                 # logic gate -> errors
│   ├── transformation.py             # standardize / cast / dedup -> Silver
│   ├── aggregation.py                # Gold summaries
│   ├── io_utils.py                   # idempotent Delta writes (overwrite + MERGE)
│   └── pipeline.py                   # end-to-end orchestration
├── notebooks/pyspark/                # APPROACH 1 — Databricks notebooks
│   ├── 00_setup.py                   #   create schema/volume + land data
│   ├── 01_bronze.py / 02_silver.py / 03_gold.py
│   └── 99_run_all.py                 #   one-click full run
├── declarative_pipeline/             # APPROACH 2 — Lakeflow Declarative Pipeline
│   ├── 00_landing_setup.py           #   run once: create volume + land data
│   └── hec_dlt_pipeline.py           #   the @dlt.table pipeline source
├── docs/architecture.md              # architecture + mermaid diagram
├── tests/test_pipeline.py            # local unit tests
├── main.py                           # local entry point
└── requirements.txt
```

---

## Step-by-step: get the code into Databricks

### A. Push this repo to GitHub (from your machine)

```bash
cd healthecommerce-task
git add .
git commit -m "Health E-Commerce medallion ETL: PySpark + declarative pipeline"
git push origin main
```

### B. Pull it into Databricks (Git folder)

1. In Databricks, go to **Workspace → Repos** (or **Workspace → Add → Git folder**).
2. **Add Git folder** → paste your GitHub repo URL → **Create**.
   - First time only: **Settings → Linked accounts → Git integration** to add a
     GitHub token.
3. To get later changes, open the Git folder → **⋯ → Pull**.

> Default Unity Catalog on Free Edition is **`workspace`**. If yours differs,
> change the `catalog` widget (notebooks) or the pipeline target (declarative).

---

## Approach 1 — Run the PySpark notebooks

1. Open `notebooks/pyspark/00_setup.py`, attach **Serverless** compute, **Run all**.
   (creates the schema + volume and lands the sample file)
2. Then either:
   - **One click:** open `99_run_all.py` → **Run all**, **or**
   - **Step through the medallion:** run `01_bronze.py` → `02_silver.py` → `03_gold.py` in order.
3. Inspect results:
   ```sql
   SELECT * FROM workspace.health_ecommerce.gold_revenue_by_category;
   SELECT * FROM workspace.health_ecommerce.gold_units_by_region;
   SELECT * FROM workspace.health_ecommerce.errors_transactions;
   ```
4. **Prove idempotency:** run `99_run_all.py` again — row counts in the Gold and
   Silver tables stay the same (no duplicates).

## Approach 2 — Run the Declarative (Lakeflow / DLT) pipeline

1. Run `declarative_pipeline/00_landing_setup.py` **once** (creates the volume +
   lands the data). Skip if you already ran the notebook `00_setup.py`.
2. Create the pipeline: **Workflows → Delta Live Tables / Lakeflow Pipelines →
   Create pipeline**.
   - **Source code:** select `declarative_pipeline/hec_dlt_pipeline.py` from the Git folder.
   - **Destination:** Unity Catalog → catalog `workspace`, **target schema `health_ecommerce_dlt`**
     (a separate schema so it won't collide with Approach 1's tables).
   - **Compute:** Serverless.
   - *(optional)* **Advanced → Configuration:** add key `hec.raw_path` =
     `/Volumes/workspace/health_ecommerce/raw/transactions.json` (the default
     already points here).
3. Click **Start**. Lakeflow builds the bronze → silver → gold DAG, shows the
   data-quality expectation metrics, and creates the tables.
4. Query the results in `workspace.health_ecommerce_dlt.*`.
5. **Idempotency:** click **Start** again — materialized views are recomputed,
   so the tables never accumulate duplicates.

---

## Run / test locally (optional, no Databricks)

Requires Python 3.9+ and a Java runtime (for Spark).

```bash
pip install -r requirements.txt

# Run the full pipeline against the local Spark catalog (2-level table names)
HEC_CATALOG="" python main.py          # PowerShell:  $env:HEC_CATALOG=""; python main.py

# Run the unit tests
pytest -q
```

---

## Design notes

- **Schema-on-read at Bronze.** Raw is read as all-STRING so ingestion never
  fails on a bad row; typing and validation happen downstream in Silver.
- **Logic gate is non-lossy.** The valid/invalid predicates are non-null by
  construction, so every input row lands in exactly one of Silver or Errors —
  nothing is silently dropped.
- **Deduplication before write.** `transaction_id` duplicates collapse to a
  single latest row, so curated tables stay clean even with messy input.
- **Idempotency by construction.** Bronze/Errors/Gold use `overwrite` (or
  materialized-view recompute); Silver uses a Delta `MERGE` keyed on
  `transaction_id`. Re-running on the same input yields identical final tables.
- **Same logic, two runtimes.** The notebook approach and the declarative
  pipeline implement the identical medallion and data-quality rules, so you can
  compare an imperative vs. declarative orchestration style.
