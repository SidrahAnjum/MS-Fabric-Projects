# nb_silver_drug_labels
#
# Silver-layer step for drug_label_enrichment (the openFDA API source).
# This table does have _ingested_at, since it's written fresh by
# nb_bronze_drug_label_api on each run — so, unlike dim_drugs, this one
# does dedupe on "most recently ingested wins," same pattern as the
# generic SQL-dimension template.

# Parameters cell
entity_name = "drug_label_enrichment"
primary_key = "ndc"
source_table = f"Bronze_LH.dbo.{entity_name}"   # Bronze_LH is schema-enabled — needs dbo in the path
target_table = entity_name                       # unqualified: Silver_LH is this notebook's default

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = spark.table(source_table)

w = Window.partitionBy(primary_key).orderBy(F.col("_ingested_at").desc())
df_clean = (
    df.withColumn("_rn", F.row_number().over(w))
      .filter("_rn = 1")
      .drop("_rn")
)

df_clean.write.format("delta").mode("overwrite").saveAsTable(target_table)
print(f"{entity_name}: {df.count()} raw -> {df_clean.count()} deduped rows")
