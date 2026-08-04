# Pharmacy fills analytics platform

A metadata-driven, multi-source Fabric data engineering project. Six entities get ingested through five genuinely different mechanisms: SQL, a OneLake shortcut, a paginated REST API, incremental file loads with a self-healed race condition, and real-time streaming. All of it routes through one dynamic pipeline that's driven entirely by a control table. The project also includes SCD Type 2 historization, a verified Direct Lake semantic model, and documented bugs found and fixed along the way.

This isn't a tutorial replica.

---

## What this is

A synthetic pharmacy claims dataset (patients, prescribers, pharmacies, plans, drugs, and about 31,000 claim fills) runs through a single, config-driven Fabric pipeline. Adding a new entity just means adding one row to a control table. Nobody has to touch the pipeline canvas.

The master pipeline handles five different ingestion mechanics in one canvas, plus a dedicated incremental-loading pipeline with a real, self-corrected concurrency bug, an event-driven trigger, real-time streaming, and a verified semantic model. Each of these has a defensible engineering decision behind it.

| Source | Mechanism | Why this way |
|---|---|---|
| `dim_prescribers`, `dim_pharmacies`, `dim_plans` | SQL Copy Data | Simulates operational master data living outside the lake |
| `dim_patients` | SQL Copy Data plus SCD Type 2 | Patients change health plans over time, so history is tracked instead of overwritten |
| `dim_drugs` | OneLake shortcut | A shared formulary reference, read live from another workspace, with zero copy and zero staleness |
| `drug_label_enrichment` | Paginated REST API (openFDA) | Pagination is handled in a notebook loop instead of chained pipeline activities, which is more robust and easier to debug |
| `fact_fills_raw` | File, true incremental | A dedicated pipeline (`pl_fills_incremental`) with file-modified-time watermarking and an automatic event-driven trigger |
| `streaming_pos_events` | Eventstream | Kept deliberately outside the batch orchestrator, since bounded and unbounded sources don't share a trigger |

---

## Architecture

```
Sources (SQL, shortcut, API, file, stream)
        |
        v
    Bronze  ->  Silver  ->  Gold  ->  Semantic model (Direct Lake)
   (raw)      (cleansed,    (star
               SCD2,         schema)
               quarantined)
```

There are two pipelines here:

```
pl_master_orchestrator                          pl_fills_incremental
  Lookup (control table)                          Lookup (watermark)
    -> Filter (active entities)                     -> compute true max file-modified time
      -> ForEach (parallel, batch=4)                     -> Copy (filtered by last-modified)
        -> Switch (source_type x load_type)                -> Silver cleanse
          -> sql_full   -> Copy -> Silver                       -> branch: update watermark (success)
          -> sql_scd2   -> Copy -> Silver SCD2                  -> branch: log run (success/failure)
          -> shortcut   -> Get Metadata -> Silver                -> branch: notify (completed)
          -> api        -> Notebook (paginated) -> Silver
  -> Invoke gold pipeline (once, after ForEach)      Triggered automatically by OneLake
  -> (per-entity notify/logging not replicated       file-created events, no manual run needed
     here, see "Error handling" below)
```

`fact_fills_raw` and `streaming_pos_events` are deliberately left out of `pl_master_orchestrator` (`is_active = false` in the control table). Each one has its own better-suited mechanism instead of being forced into the batch loop.

---

## What's in this repo

```
/data/                              synthetic dataset (patients, prescribers, pharmacies,
                                     plans, drugs, about 31K fills across multiple batch files,
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
  gold/nb_gold_build_star_schema.py         fact and dimension star schema build
  nb_setup_run_log.py                       one-time log table DDL
  nb_log_run.py                             shared success and failure logging notebook
  QA_fills_bronze_silver_gold.py            cross-layer reconciliation checks
/tests/
  test_silver_transformations.py            9 passing pytest tests
  test_scd2_helpers.py
/streaming/
  eventstream_producer.py                   REST/HTTPS producer, no SDK dependency
/pipelines/                          exported pipeline definitions, where available
/docs/
  PROJECT_STATUS.md                  the full build log, with 22 documented real bugs
  semantic-model-design.md           relationships, measures, and the type bug that
                                      surfaced there
  fabric-pharmacy-project-complete-guide.docx
  screenshots/                       pipeline canvases, semantic model relationships,
                                      Eventstream, Activator rule, DDM verification
README.md
```

---

## The dynamic pipeline

One control table (`dim_pipeline_config.csv`) drives every decision: source type, load type, sink table, and which silver notebook to call. Nothing in the pipeline canvas is hardcoded per entity.

The Switch activity routes on a combined key (`source_type` plus `load_type`) instead of a single field, because Fabric enforces a real platform restriction: Switch and If Condition activities cannot be nested inside another Switch or If. The original design called for a nested Switch to route `dim_patients` (SCD2) differently from the other SQL dimensions (full overwrite). That turned out to be structurally impossible. The fix was flattening the logic into four combined-key cases (`sql_full`, `sql_scd2`, `shortcut_shortcut`, `api_api_paginated`) instead. It's documented as bug 10 below, and it's a good example of a design that had to change because of a platform constraint discovered mid-build, not a planning mistake.

---

## True incremental loading, with a self-corrected race condition

`fact_fills_raw` gets its own dedicated pipeline, `pl_fills_incremental`, for two reasons. Incremental loading needs logic that doesn't belong in the shared orchestrator, and a file-arrival trigger on this one pipeline shouldn't re-run the other five entities every time a fills file lands.

The watermarking here works at the file level, not the row level. Copy Data can't filter rows inside a CSV by a column value. That only works against queryable sources like the SQL database. Instead, the pipeline tracks file upload time and uses Copy Data's native "Filter by last modified" setting.

There's a real bug worth mentioning here, one that was found and fixed rather than just built once and left alone. The first version set the watermark to `current_timestamp()` after each run. That's simple, but it has a genuine gap. If a new file landed during a run, after the Copy step already read the folder but before the watermark updated, that file's timestamp would end up older than the new watermark and get silently skipped, forever. The fix was to compute the watermark from the actual latest file-modified time genuinely seen, using a sequential ForEach that checks every file's real timestamp. That closes the gap instead of papering over it. It also required routing around a separate platform restriction, since Set Variable activities can't self-reference, using Microsoft's own documented temp-variable pattern.

This was verified across a real multi-run sequence: 10,080 rows, then 20,160, then 30,240, with each run only picking up its one new batch and already-processed files correctly ignored even though they sat in the same folder.

There's also an automatic, event-driven trigger, and it's confirmed working. A OneLake storage event trigger, backed by a Fabric Activator (Reflex) rule, fires `pl_fills_incremental` the moment a new file lands in `Files/raw/fact_fills_raw/`. There's no scheduled polling and no manual run. This was verified live: uploading a file with no manual pipeline interaction produced an automatic run visible in Monitoring hub.

Concurrency control was added after observing a real collision. Uploading two files close together produced two genuinely overlapping pipeline runs, a real race condition, not a theoretical one, confirmed by watching Monitoring hub show overlapping start and end times. The fix was setting the pipeline's concurrency to 1, which queues subsequent trigger-fired runs instead of running them in parallel.

---

## Error handling, logging, and notification: a reference pattern, not full coverage

This was built once, on `pl_fills_incremental`, and deliberately not replicated across the other five entities in `pl_master_orchestrator`. That's a documented scope decision, not an oversight.

There's a `pipeline_run_log` table with one row per run: status, rows processed, error detail, and timestamp.

Three independent branches come off the silver-cleansing step, each with its own dependency condition (Succeeded, Failed, or Completed). They aren't chained sequentially, since Fabric evaluates multiple incoming dependencies as a logical AND, and "Completed" specifically excludes "Skipped." A design that seemed reasonable on paper, notifying only after both success and failure logging finish, would have permanently deadlocked, since exactly one of those two branches is always skipped on any given run.

Notebooks return structured results using `mssparkutils.notebook.exit(json.dumps(...))`, which the pipeline can read downstream as `@activity(...).output.result.exitValue`.

---

## Semantic model: built and verified, with report-building out of scope

`Pharmacy_Analytics_SM` was built on `Gold_LH` in Direct Lake mode. It has 8 tables, 6 relationships forming a proper star schema, and 4 DAX measures, all verified working. A `Total Fills` breakdown sliced by `dim_drug[drug_class]` came back correct, non-blank, and correctly filtered, not just a measure returning some number.

The fact relationship uses `dim_patient_current`, not just `dim_patient`. The full SCD2 history table can have multiple rows per patient once a plan change occurs, which would cause fan-out if it were related directly to the fact table. A filtered view showing only the current row per patient restores the one-row-per-patient uniqueness the relationship actually needs.

Report pages and row-level security were not built. A verified, relationship-correct, Direct Lake semantic model is the right stopping point for a data engineering portfolio. Report and dashboard building is data analyst or BI developer territory. The closing proof moment in the demo video is a live query against `Gold_LH` rather than a report screenshot.

A real, three-layer bug got caught here. `plan_paid_amount` and four sibling currency columns were stored as plain text instead of numeric values, all the way from bronze through gold. That passed silently through every prior layer with no error, and only surfaced when the semantic model's `SUM()` measure tried to do real arithmetic on it. The fix was adding explicit type casts, which then required `overwriteSchema=true` on every downstream write in the chain, since Delta's `mergeSchema` only allows adding columns, not changing an existing column's type.

---

## Dataflow Gen2: the one deliberately low-code piece

Every other transformation in this project is code-first, using PySpark and dynamic pipelines. That was a deliberate choice to showcase data engineering skill over low-code tooling. `df_drug_reference_mart` is the one exception: a Dataflow Gen2 that blends `dim_drugs` with `drug_label_enrichment`, built specifically to demonstrate the judgment of knowing when to reach for a self-service tool instead of writing code.

The honest result: the blend produced no matches, either on the intended `ndc` join, since synthetic NDC codes never correspond to real openFDA codes, or on a normalized `generic_name` join attempted as a fallback. The mechanism itself, merge queries, transformation steps, and a Lakehouse destination, is fully demonstrated and correctly built. The match rate is a data characteristic, not a tool failure.

---

## Data quality and testing

`fact_fills_raw` is deliberately messy. It has four inconsistent date formats, mixed-case drug names, about 0.8 percent duplicate claims, some nulls, 15 orphan `prescriber_id` values, and occasional negative `days_supply` typos. Bad rows aren't dropped silently. They get routed to a `fact_fills_quarantine` table, with counts reported on every run.

There are 9 passing pytest tests against pure, I/O-free transformation functions in `silver_transformations.py` and `scd2_helpers.py`, run with a local Spark session and no live Fabric workspace required. A real bug was caught this way: the original date-parsing logic used `to_timestamp()`, which throws under Spark's ANSI mode, Fabric's runtime default, on a malformed date instead of returning null. That's exactly the kind of bad data this pipeline exists to handle. It was fixed with `try_to_timestamp()`.

---

## Security

Dynamic Data Masking was applied and verified on `pharmacy_opsdb.dbo.patients`. `first_name` and `last_name` are partially masked, `date_of_birth` is fully masked, and all three were confirmed via `sys.masked_columns` metadata. Live behavioral testing, querying as a genuinely non-privileged user, wasn't completed, since Fabric SQL database's Entra-only authentication model doesn't support lightweight SQL-auth test principals. The metadata verification is treated as sufficient, since it's the same source of truth Fabric itself uses to enforce masking.

Governance sensitivity labels were attempted but turned out to be unavailable. This tenant has no Purview label taxonomy configured, which is a separate, admin-level gap. It's documented as attempted and blocked, not skipped.

One limitation worth stating plainly: DDM only protects SQL query access. It doesn't protect Spark or notebook reads of the same underlying data, since masking is enforced at the query engine level, not in the stored bytes.

---

## Known limitations

Being upfront about these rather than glossing over them.

The dataset is fully synthetic. There's no real patient data, and the NDC codes are fabricated, so the openFDA enrichment doesn't literally join on `ndc` or `generic_name` against the live API.

The SQL source (`pharmacy_opsdb`) and the Eventstream producer are simulated for this demo.

Git integration was never actually available on this account. This is an organizational restriction that was confirmed and documented rather than assumed, and it means CI/CD deployment pipelines couldn't be built. This repo was assembled manually instead of synced automatically from Fabric.

Full Power BI report pages and row-level security weren't built. The verified semantic model stands in as proof this layer works.

The sensitivity label taxonomy isn't configured in this tenant, so governance tagging was attempted but had nothing to apply.

Error handling, logging, and notification are demonstrated on one pipeline, `pl_fills_incremental`, as a reference pattern. They aren't replicated across all six entities in the master orchestrator. That's a scope decision made explicitly, not an oversight.

---

## Why it's built this way

A few choices worth explaining rather than just listing.

API pagination is handled in a notebook instead of chained pipeline activities, since that's more robust and easier to debug than accumulating state across pipeline variables with size limits.

Shortcuts get a reachability check, not a copy. The entire point of a shortcut is that nothing needs to be copied, so the pipeline's only job is confirming it still resolves.

Streaming stays outside the batch orchestrator, since bounded and unbounded sources don't share a trigger cleanly.

Watermarking for `fact_fills_raw` works at the file level, not the row level. That's the only approach Copy Data's flat-file connector actually supports, and it's also the standard real-world pattern for file-based incremental loads regardless.

The logging setup uses two branches with independent dependency conditions instead of one chained flow. A cleaner-looking design that seemed reasonable on paper, notifying after both success and failure logging finish, would have deadlocked. This was verified against documentation before building it.

---

## Real bugs found and fixed

There are 22 of them, each with the actual error text, root cause, and fix. This isn't a curated highlight reel. The full list is in [`docs/PROJECT_STATUS.md`](docs/PROJECT_STATUS.md). A few of the more interesting ones:

Fabric's Switch and If activities can't nest inside another Switch or If. That's a documented platform restriction discovered mid-build, and it required a full redesign of the source-routing logic.

A silent, string-typed currency column passed through three entire layers, bronze, silver, and gold, with no error. It only surfaced when the semantic model tried to sum it.

A quarantine table was silently accumulating duplicate rows on every run, because append mode was used where overwrite was actually correct. This was an independent correctness bug found through repeated real use, not a one-off platform quirk.

A watermark race condition was self-identified and fixed before it could silently drop real data.

---

## Tech stack

Microsoft Fabric, including Data Factory pipelines, Lakehouse and OneLake, Notebooks with PySpark, SQL database, Dataflow Gen2, Eventstream, Activator, and semantic models with Direct Lake. Also Delta Lake, Python (Faker, requests, pytest), and Dynamic Data Masking in T-SQL.

## Setup

See [`docs/fabric-pharmacy-project-complete-guide.docx`](docs/fabric-pharmacy-project-complete-guide.docx) for the full step-by-step build, and [`docs/PROJECT_STATUS.md`](docs/PROJECT_STATUS.md) for the complete build history, every bug, and every scope decision made along the way.
