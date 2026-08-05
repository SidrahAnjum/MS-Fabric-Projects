"""
test_silver_transformations.py

Basic unit tests for the fills-cleansing functions in
notebooks/silver/silver_transformations.py. Run with:

    pytest tests/test_silver_transformations.py -v

Requires pyspark installed locally; no Fabric/Delta runtime needed
since these functions operate on plain Spark DataFrames.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "notebooks", "silver"))

import pytest
from pyspark.sql import SparkSession, Row

from silver_transformations import (
    dedupe_fills,
    parse_fill_dates,
    fix_days_supply,
    split_quarantine,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("silver-transform-tests")
        .getOrCreate()
    )


def test_dedupe_fills_removes_exact_duplicates(spark):
    df = spark.createDataFrame(
        [
            Row(fill_id="F1", patient_id="P1"),
            Row(fill_id="F1", patient_id="P1"),  # exact duplicate
            Row(fill_id="F2", patient_id="P2"),
        ]
    )
    result = dedupe_fills(df)
    assert result.count() == 2


def test_parse_fill_dates_handles_all_known_formats(spark):
    df = spark.createDataFrame(
        [
            Row(fill_date="2024-01-15"),
            Row(fill_date="01/15/2024"),
            Row(fill_date="15-Jan-2024"),
            Row(fill_date="2024-01-15T08:30:00"),
        ]
    )
    result = parse_fill_dates(df)
    parsed = [r["fill_date_parsed"] for r in result.collect()]

    assert all(p is not None for p in parsed), "every known format should parse successfully"
    # first three represent the same calendar date
    dates_only = {p.date() for p in parsed[:3]}
    assert len(dates_only) == 1
    assert "fill_date" not in result.columns, "original messy column should be dropped"


def test_parse_fill_dates_returns_null_for_unknown_format(spark):
    df = spark.createDataFrame([Row(fill_date="not-a-real-date")])
    result = parse_fill_dates(df)
    assert result.collect()[0]["fill_date_parsed"] is None


def test_fix_days_supply_corrects_negative_typos(spark):
    df = spark.createDataFrame([Row(days_supply=-30), Row(days_supply=90)])
    result = fix_days_supply(df)
    values = sorted(r["days_supply"] for r in result.collect())
    assert values == [30, 90]


def test_split_quarantine_separates_orphan_foreign_keys(spark):
    fills = spark.createDataFrame(
        [
            Row(fill_id="F1", prescriber_id="VALID1"),
            Row(fill_id="F2", prescriber_id="ORPHAN99"),
        ]
    )
    valid_prescribers = spark.createDataFrame([Row(prescriber_id="VALID1")])

    good, quarantine = split_quarantine(fills, valid_prescribers)

    assert good.count() == 1
    assert quarantine.count() == 1
    assert good.collect()[0]["fill_id"] == "F1"
    assert quarantine.collect()[0]["fill_id"] == "F2"
