"""Unit tests for the core transformation logic.

These run locally and need `pyspark` + `delta-spark` (see requirements.txt) and
a working Java runtime. They do NOT need Databricks.

    pip install -r requirements.txt
    pytest -q
"""
import pytest

from src.spark_session import get_spark
from src.transformation import to_silver
from src.validation import split_valid_invalid


@pytest.fixture(scope="session")
def spark():
    s = get_spark("hec-tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s


def _raw_rows():
    # (transaction_id, timestamp, customer_id, region, amount, quantity, category, status)
    return [
        ("T-1", "2026-05-01T10:00:00Z", "1", "North America", "10.00", "2", "Vitamins", "completed"),
        ("T-2", "2026-05-01T11:00:00+00:00", "2", "Europe", "20.00", "1", "First Aid", "completed"),
        ("T-1", "2026-05-02T10:00:00Z", "1", "North America", "10.00", "2", "Vitamins", "completed"),  # dup
        (None, "2026-05-01T10:00:00Z", "3", "Europe", "5.00", "1", "First Aid", "completed"),          # missing id
        ("", "2026-05-01T10:00:00Z", "4", "Europe", "5.00", "1", "First Aid", "completed"),            # blank id
        ("T-3", "2026-05-01T10:00:00Z", "5", "Asia Pacific", "-9.00", "1", "Pain Relief", "completed"),# negative
    ]


COLS = ["transaction_id", "timestamp", "customer_id", "region", "amount", "quantity", "category", "status"]


def test_logic_gate_quarantines_bad_rows(spark):
    df = spark.createDataFrame(_raw_rows(), COLS)
    valid, errors = split_valid_invalid(df)
    # 3 bad rows: null id, blank id, negative amount
    assert errors.count() == 3
    # valid still contains the duplicate pair (dedup happens later)
    assert valid.count() == 3
    reasons = {r.error_reason for r in errors.collect()}
    assert "missing_transaction_id" in reasons
    assert "negative_amount" in reasons


def test_silver_deduplicates_by_transaction_id(spark):
    df = spark.createDataFrame(_raw_rows(), COLS)
    valid, _ = split_valid_invalid(df)
    silver = to_silver(valid)
    # T-1 (dup) collapses to one row -> T-1, T-2 = 2 unique transactions
    assert silver.count() == 2
    ids = {r.transaction_id for r in silver.collect()}
    assert ids == {"T-1", "T-2"}


def test_silver_casts_types(spark):
    df = spark.createDataFrame(_raw_rows(), COLS)
    valid, _ = split_valid_invalid(df)
    silver = to_silver(valid)
    dtypes = dict(silver.dtypes)
    assert dtypes["customer_id"] == "int"
    assert dtypes["quantity"] == "int"
    assert dtypes["amount"] == "double"
    assert dtypes["event_timestamp"] == "timestamp"
    assert dtypes["event_date"] == "date"
