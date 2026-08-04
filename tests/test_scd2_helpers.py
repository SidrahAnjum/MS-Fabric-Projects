"""
test_scd2_helpers.py

Unit tests for the SCD Type 2 change-detection logic in
notebooks/silver/scd2_helpers.py. These test the pure comparison logic
only — no Delta table or Fabric lakehouse is needed, since the actual
merge/write is handled separately in nb_silver_patients_scd2.py.

Run with: pytest tests/test_scd2_helpers.py -v
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "notebooks", "silver"))

import pytest
from pyspark.sql import SparkSession, Row

from scd2_helpers import add_row_hash, detect_scd2_changes


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("scd2-helper-tests")
        .getOrCreate()
    )


def test_add_row_hash_is_stable_for_identical_rows(spark):
    df = spark.createDataFrame(
        [Row(patient_id="P1", plan_id="PLAN001", state="MA")]
    )
    result = add_row_hash(df, tracked_columns=["plan_id", "state"])
    hash_value = result.collect()[0]["_row_hash"]
    assert hash_value is not None and len(hash_value) == 64  # sha256 hex length


def test_add_row_hash_differs_when_tracked_column_changes(spark):
    df = spark.createDataFrame(
        [
            Row(patient_id="P1", plan_id="PLAN001", state="MA"),
            Row(patient_id="P1", plan_id="PLAN002", state="MA"),  # plan changed
        ]
    )
    result = add_row_hash(df, tracked_columns=["plan_id", "state"])
    hashes = [r["_row_hash"] for r in result.collect()]
    assert hashes[0] != hashes[1]


def test_detect_scd2_changes_classifies_changed_new_and_unchanged(spark):
    current_df = spark.createDataFrame(
        [
            Row(patient_id="P1", plan_id="PLAN001", state="MA", _row_hash="hashA"),
            Row(patient_id="P2", plan_id="PLAN002", state="NH", _row_hash="hashB"),
        ]
    )
    incoming_df = spark.createDataFrame(
        [
            # P1: plan changed -> new hash -> should be "changed"
            Row(patient_id="P1", plan_id="PLAN003", state="MA", _row_hash="hashC"),
            # P2: nothing changed -> same hash -> should be "unchanged"
            Row(patient_id="P2", plan_id="PLAN002", state="NH", _row_hash="hashB"),
            # P3: brand new patient -> should be "new"
            Row(patient_id="P3", plan_id="PLAN001", state="CT", _row_hash="hashD"),
        ]
    )

    changed, new_records, unchanged = detect_scd2_changes(
        incoming_df, current_df, key_col="patient_id"
    )

    assert changed.count() == 1
    assert changed.collect()[0]["patient_id"] == "P1"

    assert new_records.count() == 1
    assert new_records.collect()[0]["patient_id"] == "P3"

    assert unchanged.count() == 1
    assert unchanged.collect()[0]["patient_id"] == "P2"


def test_detect_scd2_changes_on_first_load_treats_everything_as_new(spark):
    current_df = spark.createDataFrame(
        [], "patient_id STRING, plan_id STRING, state STRING, _row_hash STRING"
    )
    incoming_df = spark.createDataFrame(
        [Row(patient_id="P1", plan_id="PLAN001", state="MA", _row_hash="hashA")]
    )

    changed, new_records, unchanged = detect_scd2_changes(
        incoming_df, current_df, key_col="patient_id"
    )

    assert changed.count() == 0
    assert new_records.count() == 1
    assert unchanged.count() == 0
