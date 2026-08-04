# Pharmacy fills analytics platform

A metadata-driven, multi-source Fabric data engineering project — six entities ingested through five genuinely different mechanisms (SQL, OneLake shortcut, paginated REST API, incremental file loads with a self-healed race condition, and real-time streaming), all routed through one dynamic pipeline driven entirely by a control table, with SCD Type 2 historization, a verified Direct Lake semantic model, and 22 real, documented bugs found and fixed along the way.

**This isn't a tutorial replica.** Every design decision below — including the ones that didn't work, or that hit a genuine platform restriction — is documented with the actual reasoning, not just the happy path.

---

## What this is

A synthetic pharmacy claims dataset (patients, prescribers, pharmacies, plans, drugs, and ~31,000 claim fills) run through a single, config-driven Fabric pipeline. Adding a new entity means adding one row to a control table — never touching the pipeline canvas.

The interesting part isn't the medallion architecture itself — bronze/silver/gold is table stakes. It's that the pipeline handles **five genuinely different ingestion mechanics** in one canvas, plus a dedicated incremental-loading pipeline with a real, self-corrected concurrency bug, an event-driven trigger, real-time streaming, and a verified semantic model — each with a defensible engineering decision behind it.

| Source | Mechanism | Why this way |
|---|---|---|
| `dim_prescribers`, `dim_pharmacies`, `dim_plans` | SQL Copy Data | Simulates operational master data living outside the lake |
| `dim_patients` | SQL Copy Data + **SCD Type 2** | Patients change health plans over time; history is tracked, not overwritten |
| `dim_drugs` | **OneLake shortcut** | A shared formulary reference, read live from another workspace — zero copy, zero staleness |
| `drug_label_enrichment` | **Paginated REST API** (openFDA) | Pagination handled in a notebook loop, not chained pipeline activities — more robust, easier to debug |
| `fact_fills_raw` | **File, true incremental** | A dedicated pipeline (`pl_fills_incremental`) with file-modified-time watermarking and an automatic event-driven trigger |
| `streaming_pos_events` | **Eventstream** | Deliberately outside the batch orchestrator — bounded and unbounded sources don't share a trigger |

---

## Architecture

```
Sources (SQL / shortcut / API / file / stream)
        │
        ▼
    Bronze  →  Silver  →  Gold  →  Semantic model (Direct Lake)
   (raw)    (cleansed,   (star
             SCD2,       schema)
             quarantined)
```

**Two pipelines, not one:**

```
pl_master_orchestrator                          pl_fills_incremental
  Lookup (control table)                          Lookup (watermark)
    → Filter (active entities)                       → compute true max file-modified time
      → ForEach (parallel, batch=4)                     → Copy (filtered by last-modified)
        → Switch (source_type × load_type)                → Silver cleanse
          → sql_full   → Copy → Silver                       → branch: update watermark (success)
          → sql_scd2   → Copy → Silver SCD2                  → branch: log run (success/failure)
          → shortcut   → Get Metadata → Silver                → branch: notify (completed)
          → api        → Notebook (paginated) → Silver
  → Invoke gold pipeline (once, after ForEach)      Triggered automatically by OneLake
  → (per-entity notify/logging NOT replicated       file-created events — no manual run needed
     here; see "Error handling" below)
```

`fact_fills_raw` and `streaming_pos_events` are deliberately **excluded** from `pl_master_orchestrator` (`is_active = false` in the control table) — each has its own better-suited mechanism instead of being forced into the batch loop.

---

## What's in this repo

```
/data/                              synthetic dataset (patients, prescribers, pharmacies,
                                     plans, drugs, ~31K fills across multiple batch files,
                                     streaming sample)
/config/
  dim_pipeline_config.csv           the control table driving the dynamic pipeline
  pipeline_watermark.csv            seed for the incremental-load watermark table
/notebooks/
  bronze/nb_bronze_drug_label_api.py        paginated openFDA ingestion
  silver/nb_silver_generic.py               reusable dedup template (SQL-sourced dims)
  silver/nb_silver_patients_scd2.py         SCD Type 2 historization
  silver/nb_silver_drugs.py                 shortcut-backed dimension cleanup
  silver/nb_silver_drug_labels.py           API-sourced enrichment cleanup
  silver/nb_silver_fills.py                 fills cleansing, quarantine, currency casting
  silver/silver_transformations.py          pure, unit-tested transformation functions
  silver/scd2_helpers.py                    pure, unit-tested SCD2 change-detection logic
  gold/nb_gold_build_star_schema.py         fact/dim star schema build
  nb_setup_run_log.py                       one-time log table DDL
  nb_log_run.py                             shared success/failure logging notebook
  QA_fills_bronze_silver_gold.py            cross-layer reconciliation checks
/tests/
  test_silver_transformations.py            9 passing pytest tests
  test_scd2_helpers.py
/streaming/
  eventstream_producer.py                   REST/HTTPS producer (no SDK dependency)
/pipelines/                          exported pipeline definitions (where available)
/docs/
  PROJECT_STATUS.md                  the full build log — 22 documented real bugs
  semantic-model-design.md           relationships, measures, and the type bug that
                                      surfaced there
  fabric-pharmacy-project-complete-guide.docx
  screenshots/                       pipeline canvases, semantic model relationships,
                                      Eventstream, Activator rule, DDM verification
README.md
```

---

## The dynamic pipeline

One control table (`dim_pipeline_config.csv`) drives every decision: source type, load type, sink table, which silver notebook to call. Nothing in the pipeline canvas is hardcoded per-entity.

**Switch on a combined key** (`source_type` + `load_type`), not a single field — because Fabric enforces a real platform restriction: *Switch and If Condition cannot be nested inside another Switch or If*. The original design called for a nested Switch to route `dim_patients` (SCD2) differently from the other SQL dimensions (full overwrite); that turned out to be structurally impossible. Fixed by flattening into 4 combined-key cases (`sql_full`, `sql_scd2`, `shortcut_shortcut`, `api_api_paginated`) instead — documented as bug #10 below, and a good example of a design that had to change *because* of a platform constraint discovered mid-build, not a planning mistake.

---

## True incremental loading, with a self-corrected race condition

`fact_fills_raw` gets its own dedicated pipeline, `pl_fills_incremental`, for two reasons: incremental loading needs logic that doesn't belong in the shared orchestrator, and a file-arrival trigger on this one pipeline shouldn't re-run the other five entities every time a fills file lands.

**File-level watermarking, not row-level.** Copy Data can't filter rows *inside* a CSV by a column value — that only works against queryable sources like the SQL database. Instead, the pipeline tracks **file upload time** and uses Copy Data's native "Filter by last modified" setting.

**A real bug found and fixed, not just built once and left alone**: the first version set the watermark to `current_timestamp()` after each run — simple, but with a genuine gap. If a new file landed *during* a run (after the Copy step already read the folder, but before the watermark updated), that file's timestamp would end up older than the new watermark and get silently skipped, forever. Fixed by computing the watermark from the **actual latest file-modified time genuinely seen**, via a sequential ForEach checking every file's real timestamp — closing the gap rather than papering over it. This required routing around a separate platform restriction (Set Variable activities can't self-reference) using Microsoft's own documented temp-variable pattern.

**Verified across a real multi-run sequence**: 10,080 → 20,160 → 30,240 rows, each run only picking up its one new batch, with already-processed files correctly ignored despite sitting in the same folder.

**An automatic, event-driven trigger — confirmed working.** A OneLake storage event trigger (backed by a Fabric Activator/Reflex rule) fires `pl_fills_incremental` the moment a new file lands in `Files/raw/fact_fills_raw/` — no scheduled polling, no manual run. Verified live: uploading a file with no manual pipeline interaction produced an automatic run within Monitoring hub.

**Concurrency control, added after observing a real collision.** Uploading two files close together produced two genuinely overlapping pipeline runs — a real race condition, not a theoretical one, confirmed by watching Monitoring hub show overlapping start/end times. Fixed by setting the pipeline's `Concurrency = 1`, queuing subsequent trigger-fired runs instead of running them in parallel.

---

## Error handling, logging, and notification — a reference pattern, not full coverage

Built once, on `pl_fills_incremental`, and **deliberately not replicated** across the other five entities in `pl_master_orchestrator` — a documented scope decision, not an oversight.

- **`pipeline_run_log`** table — one row per run: status, rows processed, error detail, timestamp.
- Three independent branches off the silver-cleansing step, each with its own **dependency condition** (Succeeded / Failed / Completed) — not chained sequentially, since Fabric evaluates multiple incoming dependencies as a logical AND, and "Completed" specifically excludes "Skipped." A design that seemed reasonable (notify *after* both success and failure logging finish) would have permanently deadlocked, since exactly one of those two branches is always skipped on any given run. Verified against Microsoft's documentation *before* building it, not discovered by trial and error.
- Notebooks return structured results via `mssparkutils.notebook.exit(json.dumps(...))`, readable downstream as `@activity(...).output.result.exitValue`.

---

## Semantic model — built and verified; full report-building deliberately out of scope

`Pharmacy_Analytics_SM`, Direct Lake mode on `Gold_LH` — 8 tables, 6 relationships forming a proper star schema, 4 DAX measures. Verified with a real sliced breakdown (`Total Fills` × drug class), not just "the measure returns a number."

**`dim_patient_current`, not just `dim_patient`, feeds the fact relationship** — the full SCD2 history table can have multiple rows per patient once a plan change occurs, which would cause fan-out if related directly to the fact table. A filtered, `is_current`-only view restores the one-row-per-patient uniqueness the relationship needs.

**Report pages and RLS were not built.** An org-level access restriction blocked sign-in to the Power BI service specifically — a different, narrower restriction than the ones affecting Fabric's own workspace access (which continued working fine, including for the semantic model editor itself). Treated as a deliberate stopping point: report/dashboard building sits closer to data-analyst territory than core data engineering, and the semantic model itself — built, related, measured, and verified as genuinely report-ready — already demonstrates the point of this layer. The closing "proof" moment in the demo is a live query against `Gold_LH`, not a report screenshot.

**A real, three-layer bug caught here**: `plan_paid_amount` and four sibling currency columns were stored as `STRING`, not numeric, all the way from bronze through gold — passing silently through every prior layer with no error, only surfacing when the semantic model's `SUM()` measure tried to do real arithmetic. Fixed with explicit type casts, which then required `overwriteSchema=true` on every downstream write in the chain, since Delta's `mergeSchema` only allows adding columns, not changing an existing column's type.

---

## Dataflow Gen2 — deliberately the one low-code piece

Every other transformation in this project is code-first (PySpark, dynamic pipelines) — a deliberate choice to showcase data engineering skill over low-code tooling. `df_drug_reference_mart` is the one exception: a Dataflow Gen2 blending `dim_drugs` with `drug_label_enrichment`, built specifically to demonstrate the judgment of knowing *when* to reach for a self-service tool instead of writing code.

**Honest result**: the blend produced no matches, on either the intended `ndc` join (synthetic NDC codes never correspond to real openFDA codes) or a normalized `generic_name` join attempted as a fallback. The mechanism — merge queries, transformation steps, a Lakehouse destination — is fully demonstrated and correctly built; the specific match rate is a data characteristic, not a tool failure.

---

## Data quality and testing

`fact_fills_raw` is deliberately messy: 4 inconsistent date formats, mixed-case drug names, ~0.8% duplicate claims, some nulls, 15 orphan `prescriber_id` values, occasional negative `days_supply` typos. Bad rows aren't dropped silently — they're routed to a `fact_fills_quarantine` table with counts reported on every run.

**9 passing pytest tests** against pure, I/O-free transformation functions (`silver_transformations.py`, `scd2_helpers.py`), run with a local Spark session, no live Fabric workspace required. **A real bug was caught this way**: the original date-parsing logic used `to_timestamp()`, which throws under Spark's ANSI mode (Fabric's runtime default) on a malformed date instead of returning null — exactly the kind of bad data this pipeline exists to handle. Fixed with `try_to_timestamp()`.

---

## Security

**Dynamic Data Masking**, applied and verified on `pharmacy_opsdb.dbo.patients` — `first_name`/`last_name` partially masked, `date_of_birth` fully masked, confirmed via `sys.masked_columns` metadata. Live behavioral testing (querying as a genuinely non-privileged user) wasn't completed, since Fabric SQL database's Entra-only authentication model doesn't support lightweight SQL-auth test principals — the metadata verification is treated as sufficient, since it's the same source of truth Fabric itself uses to enforce masking.

**Governance sensitivity labels** were attempted but unavailable — this tenant has no Purview label taxonomy configured, a separate admin-level gap distinct from the item-level permission restrictions hit elsewhere in this project. Documented as attempted-and-blocked, not skipped.

**Known scope limit worth stating plainly**: DDM only protects SQL query access — it does not protect Spark/notebook reads of the same underlying data, since masking is enforced at the query engine level, not in the stored bytes.

---

## Known limitations

Being upfront about these rather than glossing over them:

- The dataset is fully synthetic — no real patient data, and NDC codes are fabricated, so the openFDA enrichment doesn't literally join on `ndc` (or `generic_name`) against the live API.
- The SQL source (`pharmacy_opsdb`) and Eventstream producer are simulated for this demo.
- **Git integration was never actually available** on this account — an org-level restriction, confirmed and documented rather than assumed. This means CI/CD (deployment pipelines) couldn't be built, and this repo was assembled manually rather than synced automatically from Fabric.
- **Power BI service access is separately restricted** — full report pages and RLS weren't built as a result; the verified semantic model stands in as proof this layer works.
- **Sensitivity label taxonomy isn't configured** in this tenant — governance tagging was attempted but had nothing to apply.
- Error handling, logging, and notification are demonstrated on one pipeline (`pl_fills_incremental`) as a reference pattern, not replicated across all six entities in the master orchestrator — a scope decision made explicitly, not an oversight.

---

## Why it's built this way

A few choices worth explaining rather than just listing:

- **Notebook-based API pagination instead of chained pipeline activities.** More robust, easier to debug than accumulating state across pipeline variables with size limits.
- **Shortcuts get a reachability check, not a copy.** The entire point of a shortcut is that nothing needs to be copied — the pipeline's only job is confirming it still resolves.
- **Streaming stays outside the batch orchestrator.** Bounded and unbounded sources don't share a trigger cleanly.
- **File-level, not row-level, watermarking for `fact_fills_raw`.** The only approach Copy Data's flat-file connector actually supports — and the standard real-world pattern for file-based incremental loads regardless.
- **Two logging branches with independent dependency conditions, not one chained flow.** A cleaner design that seemed reasonable on paper (notify after both success/failure logging) would have deadlocked — verified against documentation before building it.

---

## Real bugs found and fixed

**22 of them**, each with the actual error text, root cause, and fix — not a curated highlight reel. Full list in [`docs/PROJECT_STATUS.md`](docs/PROJECT_STATUS.md). A few of the more interesting ones:

- Fabric's Switch/If activities cannot nest inside another Switch/If — a documented platform restriction discovered mid-build, requiring a full redesign of the source-routing logic.
- A silent `STRING`-typed currency column passed through three entire layers (bronze, silver, gold) with no error, only surfacing when the semantic model tried to sum it.
- A quarantine table was silently accumulating duplicate rows on every run, due to `append` mode being used where `overwrite` was actually correct — an independent correctness bug found through repeated real use, not a one-off platform quirk.
- A watermark race condition, self-identified and fixed before it could silently drop real data.

---

## Tech stack

Microsoft Fabric (Data Factory pipelines, Lakehouse/OneLake, Notebooks/PySpark, SQL database, Dataflow Gen2, Eventstream, Activator, semantic models with Direct Lake) · Delta Lake · Python (Faker, requests, pytest) · Dynamic Data Masking (T-SQL)

## Setup

See [`docs/fabric-pharmacy-project-complete-guide.docx`](docs/fabric-pharmacy-project-complete-guide.docx) for the full step-by-step build, and [`docs/PROJECT_STATUS.md`](docs/PROJECT_STATUS.md) for the complete build history, every bug, and every scope decision made along the way.
