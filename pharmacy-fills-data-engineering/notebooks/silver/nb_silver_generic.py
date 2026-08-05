# nb_silver_generic
#
# Used for the SQL-sourced dimensions that don't need SCD2 historization:
# dim_prescribers, dim_pharmacies, dim_plans. (dim_patients uses
# nb_silver_patients_scd2 instead.)
#
# Lakehouse setup: Silver_LH as default, Bronze_LH attached as a second,
# non-default lakehouse. Bronze_LH is schema-enabled, so cross-lakehouse
# reads need the `dbo` schema in the path.

# Parameters cell (Base parameters inject these from the pipeline)
entity_name = "dim_prescribers"
primary_key = "prescriber_id"

from pyspark.sql import functions as F

source_table = f"Bronze_LH.dbo.{entity_name}"
target_table = entity_name  # unqualified: Silver_LH is this notebook's default

df = spark.table(source_table)

# No _ingested_at column here — that only exists on entities that got it
# added explicitly during their bronze load (fact_fills_raw, drug_label_enrichment).
# SQL-copied dimensions don't have it, and since Copy_SQL_Dimension runs in
# Overwrite mode (full refresh each run, no accumulating history), a plain
# dedupe on the primary key is sufficient — there's no "most recent version"
# to pick between, since overwrite means only one load's worth of data exists
# at a time.
df_clean = df.dropDuplicates([primary_key])

df_clean.write.format("delta").mode("overwrite").saveAsTable(target_table)
print(f"{entity_name}: {df.count()} raw -> {df_clean.count()} deduped rows")
