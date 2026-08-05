# nb_silver_fills
#
# Silver-layer cleansing for fact_fills_raw. Uses silver_transformations.py
# for the actual logic (unit tested separately — see /tests) and handles
# the Delta I/O here.
#
# Lakehouse setup for this notebook: Silver_LH as default, Bronze_LH
# attached as a second (non-default) lakehouse — same pattern as
# nb_silver_drugs and nb_silver_drug_labels. Bronze_LH is schema-enabled,
# so cross-lakehouse references need the `dbo` schema in the path.

# Parameters cell
entity_name = "fact_fills_raw"
primary_key = "fill_id"

from pyspark.sql import functions as F

# Spark's default parser policy ("EXCEPTION") throws a SparkUpgradeException
# for certain date/time patterns where the legacy and new parsers could
# theoretically disagree, even when try_to_timestamp is used — this is a
# safety check, not a real parse failure. "CORRECTED" tells Spark to trust
# the new parser and return null on genuine failures (which is what
# try_to_timestamp is supposed to do) instead of throwing.
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")


# Inlined from silver_transformations.py (unit tested separately —
# see /tests/test_silver_transformations.py, run against the standalone
# module file, not this copy). Inlined here rather than imported because
# Fabric notebooks don't pick up a local module file automatically —
# it would need to be uploaded to Files and added to sys.path first.
def dedupe_fills(df, key_cols=("fill_id",)):
    return df.dropDuplicates(list(key_cols))


def parse_fill_dates(df, date_col="fill_date", parsed_col="fill_date_parsed"):
    return df.withColumn(
        parsed_col,
        F.coalesce(
            F.try_to_timestamp(F.col(date_col), F.lit("yyyy-MM-dd")),
            F.try_to_timestamp(F.col(date_col), F.lit("MM/dd/yyyy")),
            F.try_to_timestamp(F.col(date_col), F.lit("dd-MMM-yyyy")),
            F.try_to_timestamp(F.col(date_col), F.lit("yyyy-MM-dd'T'HH:mm:ss")),
        ),
    ).drop(date_col)


def fix_days_supply(df, col="days_supply"):
    return df.withColumn(col, F.abs(F.col(col)))


def cast_currency_columns(df, columns=("ingredient_cost", "dispensing_fee", "copay_amount", "plan_paid_amount", "total_paid_amount")):
    # Bronze's Auto create table inferred these as STRING rather than a
    # numeric type — SUM() in DAX (and Spark) can't operate on strings,
    # which surfaced as "Calculation error ... SUM cannot work with
    # values of type String" once the semantic model tried to aggregate
    # Total Plan Paid. Cast explicitly here rather than relying on
    # inference, since this is exactly the kind of silent type mismatch
    # inference can produce without erroring until something downstream
    # actually tries to do math on it.
    from pyspark.sql.types import DecimalType
    for col in columns:
        df = df.withColumn(col, F.col(col).cast(DecimalType(10, 2)))
    return df


def split_quarantine(df, valid_ids_df, join_col="prescriber_id"):
    flagged = valid_ids_df.withColumn("_valid", F.lit(True))
    joined = df.join(flagged, on=join_col, how="left")
    good = joined.filter(F.col("_valid") == True).drop("_valid")  # noqa: E712
    quarantine = joined.filter(F.col("_valid").isNull()).drop("_valid")
    return good, quarantine


df = spark.table("Bronze_LH.dbo.fact_fills_raw")
df = dedupe_fills(df)
df = parse_fill_dates(df)

# Standardize drug_name via the drug dimension instead of trusting raw casing.
# Reads from Bronze_LH (not Silver_LH) deliberately: ForEach_Entity runs
# entities in parallel (batch=4), so there's no guaranteed ordering between
# this notebook and dim_drugs' own silver step. Bronze_LH.dim_drugs is
# already clean reference data via the shortcut, so reading it directly
# here avoids a race condition rather than depending on another entity's
# silver output finishing first.
dim_drugs = spark.table("Bronze_LH.dbo.dim_drugs").select("ndc", F.col("drug_name").alias("drug_name_std"))
df = df.join(dim_drugs, on="ndc", how="left").drop("drug_name").withColumnRenamed("drug_name_std", "drug_name")

df = fix_days_supply(df)
df = cast_currency_columns(df)

valid_prescribers = spark.table("Bronze_LH.dbo.dim_prescribers").select("prescriber_id")
good, quarantine = split_quarantine(df, valid_prescribers)

good.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_fills")

# mode("overwrite"), not "append": split_quarantine() recomputes the FULL
# quarantine set fresh every run, not an incremental addition — append
# mode would have silently duplicated the same rejected rows across every
# single pipeline run forever. overwriteSchema handles the same
# column-type-drift issue as fact_fills above.
quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_fills_quarantine")

print(f"Clean rows: {good.count()}  |  Quarantined rows: {quarantine.count()}")

# Returns a structured result to the pipeline, readable via
# @activity('Notebook_SilverFills').output.result.exitValue — this is what
# the logging step reads to know how many rows this run actually processed,
# without needing a separate round-trip query against the table itself.
import json
mssparkutils.notebook.exit(json.dumps({
    "status": "success",
    "rows_processed": good.count(),
    "rows_quarantined": quarantine.count(),
}))
