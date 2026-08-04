# Synthetic pharmacy fills dataset

A synthetic (fully fabricated) healthcare/pharmacy claims dataset for demonstrating a bronze → silver → gold data engineering pipeline. No real patient data — generated with Faker + randomized business logic.

## Files

| File | Rows | Role |
|---|---|---|
| `dim_patients.csv` | 600 | Patient master (demographics, plan enrollment) |
| `dim_prescribers.csv` | 150 | Prescribing providers (NPI, specialty) |
| `dim_pharmacies.csv` | 45 | Dispensing pharmacies (retail/mail/specialty channel) |
| `dim_plans.csv` | 8 | Payer/health plan reference |
| `dim_drugs.csv` | 77 | Drug/NDC reference incl. opioid and controlled-substance flags |
| `fact_fills_raw.csv` | ~30,240 | Raw pharmacy claim fills — **bronze-quality, intentionally messy** |
| `streaming_pos_events.jsonl` | 500 | Sample "real-time" claim adjudication events (for Eventstream demo) |

## Known data quality issues (by design)

Use these to demonstrate silver-layer cleansing logic:

- **Inconsistent date formats** in `fill_date` (ISO, `MM/DD/YYYY`, `DD-Mon-YYYY`, timestamp).
- **Inconsistent text casing** in `drug_name` (upper/lower/proper case) — join on `ndc` instead, and standardize `drug_name` from the `dim_drugs` lookup.
- **Duplicate claims**: ~0.8% of fills appear twice with identical `fill_id` — a common raw-feed artifact from resubmitted claims. Deduplicate on `fill_id`.
- **Nulls** in `prescriber_id`, `reject_code`, and `copay_amount` (~1% of rows).
- **Orphan foreign keys**: 15 rows reference a `prescriber_id` that doesn't exist in `dim_prescribers` — use to demonstrate referential integrity checks / quarantine logic.
- **Sign errors**: a handful of rows have negative `days_supply` (data-entry typo) — use to demonstrate validation rules.
- **Rejected claims**: rows with a non-empty `reject_code` have `$0` cost fields — filter or handle separately in financial aggregations.

## Suggested gold-layer marts

- `fact_pharmacy_fills` — cleaned, deduplicated, conformed grain = one row per fill.
- `dim_date`, `dim_patient` (SCD2 on plan changes), `dim_drug`, `dim_prescriber`, `dim_pharmacy`.
- Analytical marts to build on top:
  - **Opioid utilization review**: fills where `dim_drugs.is_opioid = True`, grouped by patient to flag multiple prescribers/pharmacies (a real PBM use case — "doctor/pharmacy shopping" detection).
  - **Generic dispensing rate**: `is_generic` share of fills by drug class.
  - **Plan cost trend**: `plan_paid_amount` over time by `payer`.
  - **Reject rate analysis**: reject_code frequency by pharmacy/plan.
  - **Adherence proxy**: refill gap analysis for chronic-condition drug classes (statins, antidiabetics) using `fill_date` and `days_supply`.

## Regeneration

The generator script (`generate_pharmacy_data.py`) uses a fixed random seed, so re-running it reproduces the same dataset. Adjust `N_PATIENTS`, `N_FILLS`, or date ranges at the top of the script to scale the dataset up or down.
