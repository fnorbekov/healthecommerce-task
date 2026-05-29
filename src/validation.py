"""The logic gate (data-quality split).

Requirement #1: if a record is missing a transaction_id OR has a negative
amount, route it to an Error storage location. The valid records continue down
the pipeline; the pipeline as a whole still completes successfully.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _missing_id_condition():
    tid = F.col("transaction_id")
    # Null OR blank/whitespace-only id is considered missing.
    return tid.isNull() | (F.trim(tid) == F.lit(""))


def _negative_amount_condition():
    amt = F.col("amount").cast("double")
    # coalesce so an unparseable/null amount does not make the predicate null.
    return F.coalesce(amt < F.lit(0), F.lit(False))


def split_valid_invalid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Split a raw DataFrame into (valid, errors).

    The two predicates are non-null by construction, so every input row lands
    in exactly one of the two outputs — no rows are silently dropped.
    """
    missing_id = _missing_id_condition()
    negative_amount = _negative_amount_condition()

    error_reason = F.concat_ws(
        "; ",
        F.when(missing_id, F.lit("missing_transaction_id")),
        F.when(negative_amount, F.lit("negative_amount")),
    )

    errors = (
        df.filter(missing_id | negative_amount)
        .withColumn("error_reason", error_reason)
        .withColumn("quarantined_at", F.current_timestamp())
    )

    valid = df.filter(~missing_id & ~negative_amount)
    return valid, errors
