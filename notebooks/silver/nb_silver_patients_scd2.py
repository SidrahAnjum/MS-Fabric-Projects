# nb_silver_patients_scd2
#
# Historizes dim_patients using SCD Type 2 — a patient changing health
# plans (or moving state) gets a new dimension row rather than an
# overwritten one, so fact history stays attributable to the plan that
# was active at the time of each fill.
#
# Uses scd2_helpers.py for the change-detection logic (unit tested
# separately in /tests/test_scd2_helpers.py) and handles the Delta
# read/write here.

# Parameters cell (mark as "Parameters" in Fabric)
entity_name = "dim_patients"
primary_key = "patient_id"
tracked_columns = ["plan_id", "state"]  # attributes that trigger a new version
source_table = "Bronze_LH.dbo.dim_patients"  # Bronze_LH is attached but not default — needs dbo schema
target_table = "dim_patients_scd2"           # unqualified: Silver_LH is this notebook's default

from pyspark.sql import functions as F
from delta.tables import DeltaTable


# Inlined from scd2_helpers.py (unit tested separately — see
# /tests/test_scd2_helpers.py, run against the standalone module file,
# not this copy). Inlined here because Fabric notebooks don't pick up a
# local module file automatically without an extra upload/sys.path step.
def add_row_hash(df, tracked_columns, hash_col="_row_hash"):
    return df.withColumn(hash_col, F.sha2(F.concat_ws("||", *tracked_columns), 256))


def detect_scd2_changes(incoming_df, current_df, key_col, hash_col="_row_hash"):
    incoming = incoming_df.alias("src")
    current = current_df.alias("tgt")

    changed = (
        incoming.join(current, on=key_col, how="inner")
        .where(F.col(f"src.{hash_col}") != F.col(f"tgt.{hash_col}"))
        .select("src.*")
    )
    new_records = incoming_df.join(current_df.select(key_col), on=key_col, how="left_anti")
    unchanged = (
        incoming.join(current, on=key_col, how="inner")
        .where(F.col(f"src.{hash_col}") == F.col(f"tgt.{hash_col}"))
        .select("src.*")
    )
    return changed, new_records, unchanged


source_df = spark.table(source_table).dropDuplicates([primary_key])

incoming = (
    add_row_hash(source_df, tracked_columns)
    .withColumn("effective_start_date", F.current_timestamp())
    .withColumn("effective_end_date", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

if not spark.catalog.tableExists(target_table):
    # First run — everything is a new "version 1" row
    incoming.write.format("delta").saveAsTable(target_table)
    print(f"Initial load: {incoming.count()} rows written to {target_table}")
else:
    target = DeltaTable.forName(spark, target_table)
    current_df = target.toDF().filter("is_current = true")

    changed, new_records, unchanged = detect_scd2_changes(
        incoming, current_df, key_col=primary_key
    )

    changed_ids = [row[primary_key] for row in changed.select(primary_key).collect()]

    # 1. Expire the old current row for anything that changed
    if changed_ids:
        target.update(
            condition=(F.col(primary_key).isin(changed_ids)) & (F.col("is_current") == True),
            set={
                "is_current": F.lit(False),
                "effective_end_date": F.current_timestamp(),
            },
        )

    # 2. Insert new-version rows for both changed entities and brand-new ones
    to_insert = changed.unionByName(new_records)
    inserted_count = to_insert.count()
    if inserted_count > 0:
        to_insert.write.format("delta").mode("append").saveAsTable(target_table)

    print(
        f"SCD2 merge for {entity_name}: "
        f"{changed.count()} changed, {new_records.count()} new, "
        f"{unchanged.count()} unchanged, {inserted_count} rows inserted"
    )
