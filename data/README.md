# Synthetic pharmacy fills dataset

This is a synthetic, fully fabricated healthcare and pharmacy claims dataset for demonstrating a bronze to silver to gold data engineering pipeline. There's no real patient data here. It was generated with Faker plus randomized business logic.

## Files

| File | Rows | Role |
|---|---|---|
| `dim_patients.csv` | 600 | Patient master data: demographics and plan enrollment |
| `dim_prescribers.csv` | 150 | Prescribing providers: NPI and specialty |
| `dim_pharmacies.csv` | 45 | Dispensing pharmacies: retail, mail, or specialty channel |
| `dim_plans.csv` | 8 | Payer and health plan reference |
| `dim_drugs.csv` | 77 | Drug and NDC reference, including opioid and controlled-substance flags |
| `fact_fills_raw.csv` | about 30,240 | Raw pharmacy claim fills, bronze quality and intentionally messy |
| `streaming_pos_events.jsonl` | 500 | Sample real-time claim adjudication events, for the Eventstream demo |

## Known data quality issues, by design

These are meant to demonstrate silver-layer cleansing logic.

- Inconsistent date formats in `fill_date`: ISO, `MM/DD/YYYY`, `DD-Mon-YYYY`, and full timestamp all appear.
- Inconsistent text casing in `drug_name`, mixing upper, lower, and proper case. Join on `ndc` instead, and standardize `drug_name` from the `dim_drugs` lookup.
- Duplicate claims. About 0.8 percent of fills appear twice with an identical `fill_id`, a common raw-feed artifact from resubmitted claims. Deduplicate on `fill_id`.
- Nulls in `prescriber_id`, `reject_code`, and `copay_amount`, affecting about 1 percent of rows.
- Orphan foreign keys. 15 rows reference a `prescriber_id` that doesn't exist in `dim_prescribers`. Use these to demonstrate referential integrity checks or quarantine logic.
- Sign errors. A handful of rows have negative `days_supply` values, simulating a data-entry typo. Use these to demonstrate validation rules.
- Rejected claims. Rows with a non-empty `reject_code` have zeroed-out cost fields. Filter these or handle them separately in financial aggregations.

## Suggested gold-layer marts

- `fact_pharmacy_fills`: cleaned, deduplicated, with a conformed grain of one row per fill.
- Dimensions: `dim_date`, `dim_patient` (with SCD2 for plan changes), `dim_drug`, `dim_prescriber`, and `dim_pharmacy`.
- Analytical marts worth building on top of these:
  - An opioid utilization review: fills where `dim_drugs.is_opioid = True`, grouped by patient to flag multiple prescribers or pharmacies. This is a real PBM use case, sometimes called doctor or pharmacy shopping detection.
  - A generic dispensing rate: the `is_generic` share of fills by drug class.
  - A plan cost trend: `plan_paid_amount` over time, by payer.
  - A reject rate analysis: reject code frequency by pharmacy and plan.
  - An adherence proxy: refill gap analysis for chronic-condition drug classes such as statins and antidiabetics, using `fill_date` and `days_supply`.

## Regeneration

The generator script, `generate_pharmacy_data.py`, uses a fixed random seed, so re-running it reproduces the same dataset. Adjust `N_PATIENTS`, `N_FILLS`, or the date ranges at the top of the script to scale the dataset up or down.
