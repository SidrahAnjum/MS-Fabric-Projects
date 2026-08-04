"""
silver_transformations.py

Pure, testable transformation functions used by nb_silver_fills.
Kept separate from the notebook itself so they can be unit tested with
plain pytest + a local Spark session, without needing a live Fabric
lakehouse or Delta tables.

Production note: when parse_fill_dates() runs on a real Fabric Spark
cluster, the session needs
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
set beforehand. Without it, Spark's default "EXCEPTION" policy can throw
a SparkUpgradeException for certain valid-looking timestamp strings even
though try_to_timestamp is used — a parser-ambiguity safety check, not a
real parse failure. This didn't surface in local pytest runs (a fresh
local SparkSession doesn't hit the same ambiguity condition), only in
the Fabric runtime — worth remembering as a category of bug that unit
tests alone won't always catch, since it depends on cluster session
config, not just the transformation logic itself.
"""

from pyspark.sql import functions as F


def dedupe_fills(df, key_cols=("fill_id",)):
    """Drop exact duplicate claims on the given key column(s)."""
    return df.dropDuplicates(list(key_cols))


def parse_fill_dates(df, date_col="fill_date", parsed_col="fill_date_parsed"):
    """
    Standardize a column containing inconsistent date string formats
    (ISO, MM/DD/YYYY, DD-Mon-YYYY, ISO timestamp) into a single timestamp
    column. Unparseable values become null rather than raising.

    Uses try_to_timestamp rather than to_timestamp: under Spark's ANSI
    mode (the Fabric runtime default), to_timestamp throws on a string
    that doesn't match its format instead of returning null, which
    would crash the whole notebook on a single malformed date. This
    was caught by test_parse_fill_dates_returns_null_for_unknown_format
    failing against the original to_timestamp implementation.
    """
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
    """Correct sign-error data-entry typos (negative days_supply)."""
    return df.withColumn(col, F.abs(F.col(col)))


def split_quarantine(df, valid_ids_df, join_col="prescriber_id"):
    """
    Split a DataFrame into (good, quarantine) based on whether join_col
    exists in valid_ids_df. Rows with no match are quarantined rather
    than silently dropped.
    """
    flagged = valid_ids_df.withColumn("_valid", F.lit(True))
    joined = df.join(flagged, on=join_col, how="left")
    good = joined.filter(F.col("_valid") == True).drop("_valid")  # noqa: E712
    quarantine = joined.filter(F.col("_valid").isNull()).drop("_valid")
    return good, quarantine
