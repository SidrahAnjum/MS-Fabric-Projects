# nb_silver_drugs
#
# Silver-layer step for dim_drugs. Unlike the SQL-sourced dimensions,
# this table has no _ingested_at column to window over (it's reference
# data read live through a OneLake shortcut, not loaded via a Copy
# activity) — so there's no "most recent load wins" logic needed here,
# just a straightforward dedupe on the natural key and a landing spot
# in the silver lakehouse for downstream consumers to depend on
# consistently, the same as every other silver dimension.

# Parameters cell
entity_name = "dim_drugs"
primary_key = "ndc"
source_table = "Bronze_LH.dbo.dim_drugs"   # Bronze_LH is schema-enabled — needs the dbo schema in the path
target_table = entity_name             # Silver_LH is this notebook's default Lakehouse

df = spark.table(source_table).dropDuplicates([primary_key])

df.write.format("delta").mode("overwrite").saveAsTable(target_table)
print(f"{entity_name}: wrote {df.count()} deduped rows to {target_table}")
