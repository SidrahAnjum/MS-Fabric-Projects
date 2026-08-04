# nb_gold_build_star_schema
#
# Builds the gold-layer star schema: fact_pharmacy_fills plus its
# dimension tables, in Gold_LH.
#
# Lakehouse setup: Gold_LH as default, Silver_LH attached as a second,
# non-default lakehouse. Both Silver_LH and Bronze_LH are schema-enabled
# (dbo), so any cross-lakehouse reference needs the full
# Lakehouse.dbo.table path.
#
# Unlike the silver notebooks, this one is safe to depend on OTHER
# entities' silver output — it's called via Execute Pipeline AFTER
# ForEach_Entity completes entirely, not from inside the parallel loop,
# so there's no race condition here the way there would be if this ran
# per-entity alongside everything else.

from pyspark.sql import functions as F

# ---- Read silver tables (all from Silver_LH, schema-qualified) ----
fills = spark.table("Silver_LH.dbo.fact_fills")
drugs = spark.table("Silver_LH.dbo.dim_drugs")
patients = spark.table("Silver_LH.dbo.dim_patients_scd2")
prescribers = spark.table("Silver_LH.dbo.dim_prescribers")
pharmacies = spark.table("Silver_LH.dbo.dim_pharmacies")
plans = spark.table("Silver_LH.dbo.dim_plans")

# ---- Build the fact table: fills + drug attributes needed for analysis ----
fact = fills.join(
    drugs.select("ndc", "drug_class", "is_opioid", "is_controlled_substance", "is_generic"),
    on="ndc",
    how="left",
)

fact_gold = fact.withColumn(
    "date_key", F.date_format("fill_date_parsed", "yyyyMMdd").cast("int")
)

fact_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_pharmacy_fills")
print(f"fact_pharmacy_fills: wrote {fact_gold.count()} rows")

# ---- dim_patient_current: filtered to is_current only, restoring
# uniqueness on patient_id — dim_patient (the full SCD2 history) can
# have multiple rows per patient_id once a patient has a second version,
# which would cause fan-out if related directly to the fact table. ----
dim_patient_current = patients.filter("is_current = true")
dim_patient_current.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_patient_current")
print(f"dim_patient_current: wrote {dim_patient_current.count()} rows")

# ---- Build dim_date from the distinct dates actually present in the fact table ----
dim_date = (
    fact_gold.select(F.col("fill_date_parsed").alias("date"), "date_key")
    .distinct()
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
)
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_date")
print(f"dim_date: wrote {dim_date.count()} rows")

# ---- Copy the remaining silver dimensions through to gold unchanged ----
# (Gold versions exist so downstream consumers — Power BI, ad hoc queries —
# only ever need to point at Gold_LH, not reach back into Silver_LH.)
dim_copies = {
    "dim_patient": patients,
    "dim_prescriber": prescribers,
    "dim_pharmacy": pharmacies,
    "dim_plan": plans,
    "dim_drug": drugs,
}
for gold_name, df in dim_copies.items():
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_name)
    print(f"{gold_name}: wrote {df.count()} rows")

# ---- drug_label_enrichment: carried through as its own standalone gold
# table rather than joined into the fact table. The synthetic dim_drugs
# NDC values won't literally match real NDC codes from the live openFDA
# API, so this stays a separate reference table for its own report tile
# rather than a strict join key against fact_pharmacy_fills — a known,
# documented limitation rather than an oversight. ----
drug_labels = spark.table("Silver_LH.dbo.drug_label_enrichment")
drug_labels.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("drug_label_enrichment")
print(f"drug_label_enrichment: wrote {drug_labels.count()} rows")

print("Gold layer build complete.")
