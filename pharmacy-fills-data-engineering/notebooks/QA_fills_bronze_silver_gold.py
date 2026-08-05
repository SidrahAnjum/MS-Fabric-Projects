# QA_fills_bronze_silver_gold
#
# Reconciles fact_fills_raw across bronze -> silver -> gold, checking
# that the numbers actually add up rather than assuming they do because
# the last run finished green. Run with Bronze_LH as default, Silver_LH
# and Gold_LH attached as additional lakehouses.

from pyspark.sql import functions as F

# ---- 1. Row counts across all three layers ----
bronze_count = spark.sql("SELECT COUNT(*) AS c FROM fact_fills_raw").collect()[0]["c"]
silver_good_count = spark.sql("SELECT COUNT(*) AS c FROM Silver_LH.dbo.fact_fills").collect()[0]["c"]
silver_quarantine_count = spark.sql("SELECT COUNT(*) AS c FROM Silver_LH.dbo.fact_fills_quarantine").collect()[0]["c"]
gold_count = spark.sql("SELECT COUNT(*) AS c FROM Gold_LH.dbo.fact_pharmacy_fills").collect()[0]["c"]

print("=== Row counts ===")
print(f"Bronze (fact_fills_raw):           {bronze_count}")
print(f"Silver clean (fact_fills):         {silver_good_count}")
print(f"Silver quarantine:                 {silver_quarantine_count}")
print(f"Silver clean + quarantine:         {silver_good_count + silver_quarantine_count}")
print(f"Gold (fact_pharmacy_fills):         {gold_count}")
print()
print("Expected: bronze >= (silver clean + quarantine), since dedup removes exact")
print("duplicate fill_ids before the good/quarantine split happens.")
print("Expected: gold count == silver clean count (gold only adds drug attribute")
print("columns via a left join, it doesn't aggregate or filter further).")

# ---- 2. Does gold's row count match silver's clean count exactly? ----
if gold_count != silver_good_count:
    print(f"\n⚠ MISMATCH: gold ({gold_count}) != silver clean ({silver_good_count})")
    print("Likely cause: the join to dim_drugs on ndc is duplicating rows —")
    print("check dim_drugs.ndc is actually unique.")
else:
    print(f"\n✓ Gold row count matches silver clean count exactly ({gold_count})")

# ---- 3. Null check on date_key (fills whose fill_date failed to parse) ----
null_date_key = spark.sql(
    "SELECT COUNT(*) AS c FROM Gold_LH.dbo.fact_pharmacy_fills WHERE date_key IS NULL"
).collect()[0]["c"]
print(f"\n=== Null date_key in gold: {null_date_key} rows ===")
print("(Expected to be low/zero — a null here means fill_date couldn't be parsed")
print("by any of the four known formats in parse_fill_dates().)")

# ---- 4. Currency columns: confirm they're actually numeric now, not string ----
schema_check = spark.sql("DESCRIBE Gold_LH.dbo.fact_pharmacy_fills").collect()
currency_cols = {"ingredient_cost", "dispensing_fee", "copay_amount", "plan_paid_amount", "total_paid_amount"}
print("\n=== Currency column types in gold ===")
for row in schema_check:
    if row["col_name"] in currency_cols:
        status = "✓" if "decimal" in row["data_type"].lower() else "⚠ STILL WRONG TYPE"
        print(f"{status} {row['col_name']}: {row['data_type']}")

# ---- 5. Sum reconciliation: total plan_paid_amount should match between silver and gold ----
silver_total_paid = spark.sql(
    "SELECT SUM(plan_paid_amount) AS total FROM Silver_LH.dbo.fact_fills"
).collect()[0]["total"]
gold_total_paid = spark.sql(
    "SELECT SUM(plan_paid_amount) AS total FROM Gold_LH.dbo.fact_pharmacy_fills"
).collect()[0]["total"]
print(f"\n=== Total plan_paid_amount ===")
print(f"Silver: {silver_total_paid}")
print(f"Gold:   {gold_total_paid}")
print("✓ Match" if silver_total_paid == gold_total_paid else "⚠ MISMATCH — investigate the drug join")

# ---- 6. Quarantine reason check: confirm quarantined rows genuinely have orphan prescriber_ids ----
orphan_check = spark.sql("""
    SELECT COUNT(*) AS c
    FROM Silver_LH.dbo.fact_fills_quarantine q
    LEFT JOIN Bronze_LH.dbo.dim_prescribers p ON q.prescriber_id = p.prescriber_id
    WHERE p.prescriber_id IS NOT NULL
""").collect()[0]["c"]
print(f"\n=== Quarantine sanity check ===")
print(f"Quarantined rows that actually DO have a valid prescriber_id: {orphan_check}")
print("(Should be 0 — if not, something other than orphan prescribers is causing quarantine.)")

print("\n=== QA complete ===")
