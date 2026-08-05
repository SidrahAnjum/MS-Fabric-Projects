"""
scd2_helpers.py

Pure change-detection logic for Slowly Changing Dimension Type 2
handling, kept separate from the Delta I/O so it can be unit tested
with plain pytest + a local Spark session (no Delta/Fabric runtime
required to test the *logic*).

The actual notebook (nb_silver_patients_scd2.py) imports these
functions and handles the Delta merge/write against the real lakehouse.
"""

from pyspark.sql import functions as F


def add_row_hash(df, tracked_columns, hash_col="_row_hash"):
    """
    Add a hash of the tracked (historized) columns so change detection
    is a single column comparison instead of comparing N columns.
    """
    return df.withColumn(hash_col, F.sha2(F.concat_ws("||", *tracked_columns), 256))


def detect_scd2_changes(incoming_df, current_df, key_col, hash_col="_row_hash"):
    """
    Compare an incoming batch against the currently-active dimension
    rows and classify each incoming row as one of:

      - changed:   key exists in current_df, but the row hash differs
                    -> the existing row should be expired, this row inserted
      - new:       key does not exist in current_df at all
                    -> insert as a new record, nothing to expire
      - unchanged: key exists and the row hash matches
                    -> no action needed

    Returns (changed_df, new_df, unchanged_df).
    """
    incoming = incoming_df.alias("src")
    current = current_df.alias("tgt")

    changed = (
        incoming.join(current, on=key_col, how="inner")
        .where(F.col(f"src.{hash_col}") != F.col(f"tgt.{hash_col}"))
        .select("src.*")
    )

    new_records = incoming_df.join(
        current_df.select(key_col), on=key_col, how="left_anti"
    )

    unchanged = (
        incoming.join(current, on=key_col, how="inner")
        .where(F.col(f"src.{hash_col}") == F.col(f"tgt.{hash_col}"))
        .select("src.*")
    )

    return changed, new_records, unchanged
