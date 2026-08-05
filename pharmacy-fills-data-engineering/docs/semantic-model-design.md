# Semantic model design: Pharmacy_Analytics_SM

This document captures the semantic model's design as written documentation, since a Fabric semantic model isn't something you can simply download the way you can a notebook or a CSV. It preserves the design even if live access to the Fabric workspace is ever lost.

## Tables included

`fact_pharmacy_fills`, `dim_date`, `dim_patient_current`, `dim_prescriber`, `dim_pharmacy`, `dim_plan`, `dim_drug`, and `drug_label_enrichment` (which stands alone with no relationship; see below).

## Relationships

This follows a standard star schema, with each dimension's "one" side pointing to the fact table's "many" side.

| Dimension | Column | Fact table | Column |
|---|---|---|---|
| `dim_patient_current` | `patient_id` | `fact_pharmacy_fills` | `patient_id` |
| `dim_prescriber` | `prescriber_id` | `fact_pharmacy_fills` | `prescriber_id` |
| `dim_pharmacy` | `pharmacy_id` | `fact_pharmacy_fills` | `pharmacy_id` |
| `dim_plan` | `plan_id` | `fact_pharmacy_fills` | `plan_id` |
| `dim_drug` | `ndc` | `fact_pharmacy_fills` | `ndc` |
| `dim_date` | `date_key` | `fact_pharmacy_fills` | `date_key` |

All six relationships use cardinality **One to many** and cross-filter direction **Single**. **Assume referential integrity** is turned off on purpose: `fact_pharmacy_fills.date_key` can be null for rows whose `fill_date` failed to parse, so referential integrity can't be safely assumed.

`drug_label_enrichment` is intentionally left unrelated. No relationship is drawn to `fact_pharmacy_fills`, because the synthetic NDC values in `dim_drugs` and `fact_pharmacy_fills` don't reliably match real NDC codes from the live openFDA API. It's meant to support its own standalone reference tile rather than being joined into fact-level analysis.

## Why dim_patient_current exists, not just dim_patient

`dim_patient` is the full SCD2 history table, and it can have multiple rows per `patient_id` once a patient has a second tracked-attribute version, such as a plan or state change. Relating that table directly to the fact table would risk fan-out, where one fill matches multiple patient rows and silently inflates counts. `dim_patient_current` solves this: it's `dim_patient` filtered down to `is_current = true` only, which restores one row per patient and is what the fact table actually relates to. The full history table still exists separately in `Gold_LH` for anyone who wants to analyze plan-change history over time.

## Measures

All four are DAX measures defined on `fact_pharmacy_fills`.

```dax
Total Fills = COUNTROWS(fact_pharmacy_fills)

Total Plan Paid = SUM(fact_pharmacy_fills[plan_paid_amount])

Opioid Fills = 
CALCULATE(COUNTROWS(fact_pharmacy_fills), dim_drug[is_opioid] = TRUE())

Generic Dispensing Rate = 
DIVIDE(
    CALCULATE(COUNTROWS(fact_pharmacy_fills), dim_drug[is_generic] = TRUE()),
    COUNTROWS(fact_pharmacy_fills)
)
```

`Generic Dispensing Rate` returns a raw decimal between 0 and 1, so it should be formatted as a percentage in the model.

## Verification performed

`Total Fills` was sliced by `dim_drug[drug_class]` and checked against expected results. The breakdown came back correct and non-blank across every class (for example, Antibiotic at 2,330 and Benzodiazepine at 2,340), summing to roughly 29,896. That's consistent with the 30,240 raw rows minus whatever got quarantined or had no matching drug. This confirmed the `dim_drug` relationship is genuinely filtering the data, not just sitting there decoratively.

## A real bug found and fixed during this build

`plan_paid_amount`, along with four sibling currency columns (`ingredient_cost`, `dispensing_fee`, `copay_amount`, `total_paid_amount`), were stored as plain text rather than numeric values all the way from bronze through gold. The `Total Plan Paid` measure's `SUM()` failed with "The function SUM cannot work with values of type String." This had passed silently through bronze, silver, and gold without ever throwing an error. It only surfaced once the semantic model actually tried to do arithmetic on the column.

The fix was adding explicit `DecimalType(10, 2)` casts in `nb_silver_fills`. That in turn required adding `overwriteSchema=true` to every downstream write, including `fact_fills`, `fact_fills_quarantine`, and every table in `nb_gold_build_star_schema`. Delta's `mergeSchema` option allows adding new columns but won't let you change an existing column's type, which is exactly what was needed once those tables already existed with the wrong types baked in.

## Report status

Report pages and row-level security were not built. This was a deliberate stopping point rather than an oversight: report and dashboard building sits closer to data analyst or BI developer work than core data engineering, and the semantic model itself, already built, related, measured, and verified, demonstrates that the pipeline produces genuinely report-ready output. That was the actual point of this layer for a data engineering portfolio.
