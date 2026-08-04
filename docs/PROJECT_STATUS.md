# Project status: Pharmacy fills analytics platform

A running record of what's built, what's working, and what's left — this is the **final build entry**. DDM applied and verified; sensitivity labels attempted and found unavailable in this tenant's configuration (documented below). All planned net-new building is complete; what remains is packaging (video, repo assembly, README finalization) and items explicitly out of scope.

---

## Goal

A portfolio-grade Microsoft Fabric data engineering project demonstrating a **metadata-driven pipeline** pulling from multiple genuinely different source types — not a single-source tutorial replica.

## Architecture

Medallion pattern — **Bronze → Silver → Gold → semantic model** (not "→ Power BI report" — see the scope note below) — with bronze pulling from five different source mechanics, each handled differently:

| Entity | Source type | Mechanism |
|---|---|---|
| `dim_prescribers`, `dim_pharmacies`, `dim_plans` | SQL | Copy Data from `pharmacy_opsdb` (Fabric-native SQL database) |
| `dim_patients` | SQL | Same source, routed to SCD Type 2 historization instead of overwrite |
| `dim_drugs` | Shortcut | Zero-copy OneLake shortcut to `Formulary_Reference_LH` in workspace `Shared_Reference_Data` |
| `drug_label_enrichment` | API | Paginated openFDA calls, handled inside a notebook |
| `fact_fills_raw` | File | CSV drop, Copy Data, incremental load type |
| `streaming_pos_events` | Eventstream | Outside the batch pipeline entirely |

## Semantic model — built and verified; report-building out of scope

`Pharmacy_Analytics_SM` was built on `Gold_LH` in Direct Lake mode — 8 tables, 6 relationships forming a proper star schema, 4 DAX measures, all verified working (a `Total Fills` × `dim_drug[drug_class]` breakdown confirmed real, non-blank, correctly-filtered results). Full details, including the relationship table, measure DAX, and a real type bug found and fixed along the way, are in `semantic-model-design.md`.

**Report pages and RLS were not built.** A verified, relationship-correct, Direct Lake semantic model is the appropriate stopping point for a data engineering portfolio — report/dashboard building is data analyst/BI developer territory. The closing "proof" moment for the demo video is a live query against `Gold_LH` (the opioid utilization review) rather than a report screenshot.

## Error handling, logging, and notification — built and verified on `pl_fills_incremental`, deliberately not replicated elsewhere

Built as a **reference implementation** on one pipeline, then deliberately left there rather than copied across `pl_master_orchestrator`'s 6 entities — a scope decision, documented explicitly rather than left implicit.

**`pipeline_run_log`** (`Bronze_LH`) — one row per run: `run_id`, `pipeline_name`, `entity_name`, `status`, `rows_processed`, `error_message`, `run_timestamp`. Created via DDL in a one-time setup notebook (`nb_setup_run_log`), not the CSV-seed pattern used for `pipeline_config`/`pipeline_watermark`.

**The mechanism**: `nb_silver_fills` returns a structured result via `mssparkutils.notebook.exit(json.dumps({...}))` — readable downstream as `@activity('Notebook_SilverFills').output.result.exitValue`. Three independent activities branch off `Notebook_SilverFills`, each with its own dependency condition:
- `Notebook_LogRun_Success` (Succeeded) — reads the real exit value, logs `rows_processed`
- `Notebook_LogRun_Failure` (Failed) — logs a static message pointing to Monitoring hub for detail, rather than fighting a validator restriction on referencing `.Error` from a Failed-dependency activity
- `Notify` (Completed) — an Office 365 Outlook activity, one-time Connection object required (same pattern as Invoke Pipeline's connection requirement)

**A real design mistake caught and corrected before building the wrong thing**: the original instinct was to chain `Notify` *after* both log activities, so notification happens once logging is done. Verified against Microsoft's own documentation that this doesn't work: multiple incoming dependencies are evaluated as a **logical AND**, and "Completed" specifically means Succeeded-or-Failed — it does **not** include Skipped. Since exactly one of the two log activities is always Skipped (they're mutually exclusive), a Notify depending on both via Completed would permanently deadlock, since the skipped one could never satisfy that condition. Fixed by connecting `Notify` directly and independently off `Notebook_SilverFills`, sidestepping the multi-dependency AND entirely.

**Testing**: multiple small incremental batch files (batch4 through batch10, 20-1000 rows each) generated specifically to re-trigger `pl_fills_incremental` repeatedly without reusing already-processed data, confirming the log table and Notify both fire correctly across several runs. The `Notify` connection was initially missing from the canvas entirely on the first test (an activity with no incoming connection simply never runs, and doesn't fail the pipeline either — it's silently skipped) — caught by checking Monitoring hub's activity list didn't include it at all, not by an error.

## Dataflow Gen2 — built, demonstrates the tool correctly; the specific blend produced no matches

`df_drug_reference_mart` merges `dim_drugs` (`Silver_LH`) with `drug_label_enrichment` (`Silver_LH`) via Power Query's Merge queries UI, writing to `Gold_LH.dbo.dim_drug_reference_mart`. Attempted first on `ndc` (predictably all-null, per the known synthetic-vs-real NDC mismatch documented since the gold layer was built), then retried on normalized (`lowercase` + `Trim`) `generic_name` — still no matches, most likely due to formatting/naming differences between the synthetic dataset's simplified drug names and openFDA's real-world naming conventions. Documented honestly as a data-characteristic limitation, not a tool failure — the actual point (demonstrating Dataflow Gen2's merge/transform/destination mechanics, and the judgment of knowing when to reach for low-code vs. code) stands regardless of match rate.

## A real bug worth highlighting: silent type mismatch across three layers

`plan_paid_amount` and four sibling currency columns were stored as **STRING**, not numeric, from bronze all the way through gold — passing silently through every layer with no error, only surfacing when the semantic model's `SUM()` measure tried to do real arithmetic on the column (`"The function SUM cannot work with values of type String"`). Fixed with explicit `DecimalType(10,2)` casts in `nb_silver_fills`, which then required adding `overwriteSchema=true` to every downstream write in the chain (`fact_fills`, `fact_fills_quarantine`, and every table `nb_gold_build_star_schema` writes) — Delta's `mergeSchema` only allows adding new columns, not changing an existing column's type, so each table in the chain needed the same fix individually. Good concrete example of "runs without error" not meaning "correct" — same lesson as the ANSI-mode date bug, surfacing in a different layer this time.

## Dynamic Data Masking — applied and verified on `pharmacy_opsdb.dbo.patients`

```sql
ALTER TABLE dbo.patients ALTER COLUMN first_name ADD MASKED WITH (FUNCTION = 'partial(1, "XXXXXXX", 0)');
ALTER TABLE dbo.patients ALTER COLUMN last_name ADD MASKED WITH (FUNCTION = 'partial(1, "XXXXXXX", 0)');
ALTER TABLE dbo.patients ALTER COLUMN date_of_birth ADD MASKED WITH (FUNCTION = 'default()');
```

Verified via `sys.masked_columns` metadata (screenshot captured) — all three columns confirmed correctly configured.

**Live behavioral testing (querying as a genuinely non-privileged user) wasn't completed** — Fabric SQL database only supports Microsoft Entra ID authentication, with no SQL-auth logins at all, so `CREATE USER ... WITHOUT LOGIN` (the standard SQL Server pattern for a lightweight test principal) isn't supported; any test user must be a real Entra identity via `CREATE USER [email] FROM EXTERNAL PROVIDER`, and no second account was available to test with. The metadata verification is treated as sufficient proof — it's the same source of truth Fabric itself uses to enforce masking, not merely a claim that the T-SQL executed without error.

**Not extended to the Lakehouse SQL analytics endpoint** — applied at the true source (`pharmacy_opsdb`) only, per the original plan's priority; the reminder that DDM only protects SQL query access (not Spark/notebook reads of the same underlying data) remains the key caveat to state alongside this in the README.

## Governance tags — attempted, unavailable in this tenant

Right-clicking `pharmacy_opsdb` for **Apply sensitivity label** showed no label options at all — meaning this tenant has no Purview sensitivity label taxonomy configured, a separate admin-level setup distinct from the item-level permission restrictions hit throughout this project (Azure resources, Git integration, Power BI service access). Not something fixable from within the workspace; would require a tenant admin configuring Purview Information Protection first. Documented as attempted-but-blocked rather than skipped — the correct native mechanism (`Apply sensitivity label`, item-level) was identified and tried, it's the taxonomy itself that doesn't exist here.

---

# Project complete — remaining items are packaging or explicitly out of scope

All planned net-new building is done. What's left:
- **Packaging**: video assembly from captured clips, manual GitHub repo assembly (Git integration blocked), final README pass
- **Explicitly out of scope**: full Power BI report/RLS (org access restriction), CI/CD (Git integration blocked), error handling/logging replicated beyond the one reference pipeline (deliberate scope decision), governance tags beyond the attempt documented above (tenant configuration gap, not fixable from the workspace)

---

## Git integration — not actually available

Contrary to earlier assumptions in this project's guide, **Git integration was never enabled** — it's blocked on this account, likely by the same kind of org-level restriction that blocked Azure resource creation, though it could also be a separate tenant-level toggle specifically for Git integration. This means everything built has **no version history and no repo backup** through Fabric's native mechanism.

**Current substitute:** every notebook and the control table exists as a delivered file from the conversation where it was built — treat those as the de facto version history until this is resolved. Re-export/re-save current state after any significant change.

**To actually fix this:** ask whoever administers the Fabric tenant specifically about the "Git integration" setting (distinct from general Azure resource permissions) — it may be a narrower ask than the Azure SQL Database restriction was.

## Why Fabric-native SQL database instead of Azure SQL Database

The original plan used Azure SQL Database. Pivoted to a native **SQL database in Fabric** item after hitting an org-level Azure resource-creation restriction ("I don't have permission to access this resource") — the Fabric-native option needs only workspace permissions already in hand, not Azure Resource Manager access.

## The dataset

Fully synthetic (Faker-generated, fixed seed, reproducible via `generate_pharmacy_data.py`):
- 600 patients, 150 prescribers, 45 pharmacies, 8 plans, 77 drug/strength rows
- ~30,240 pharmacy fills, 500-row streaming sample

`fact_fills_raw` is deliberately messy on purpose: 4 inconsistent date formats, mixed-case drug names, ~0.8% duplicate claims, some nulls, 15 orphan prescriber IDs, occasional negative `days_supply` typos — so the silver layer has real cleansing work to demonstrate, not synthetic-clean data.

## The control table (`dim_pipeline_config.csv`)

The single file driving the whole pipeline. Edited several times as the design evolved:
1. Added SQL sourcing for four dimensions (from an original all-file design)
2. Added the `dim_drugs` shortcut (ADLS Gen2 path → corrected to OneLake cross-workspace path after switching approach → corrected again after the workspace was renamed `Shared_Reference_Data`)
3. Added `drug_label_enrichment` as a new API-sourced row
4. Changed `dim_patients` to `load_type = scd2`
5. `streaming_pos_events` briefly set to `source_type = kafka`, then reverted back to `eventstream`
6. Fixed a real bug: `fact_fills_raw`'s path included a redundant `Files/` prefix, causing "File Not Found" once the Copy activity's Root folder was already set to `Files`
7. `fact_fills_raw` set to `is_active = false` — extracted out of the master orchestrator entirely into its own dedicated pipeline (see below), the same pattern already used for `streaming_pos_events`, just for a different underlying reason (dedicated incremental/trigger logic vs. bounded/unbounded mismatch)

## `pl_fills_incremental` — dedicated pipeline with true watermark-based incremental loading

Extracted out of `pl_master_orchestrator`'s `file_incremental` Switch case entirely, so a fills-file arrival doesn't re-run all 7 entities just to process one new file. This is real incremental loading, not just Append — the pipeline only processes files uploaded since the last successful run, verified across a genuine multi-run test (10,080 → 20,160 → 30,240 rows across 3 separate runs, each only picking up its one new batch).

```
Lookup_GetWatermark → Set_InitMaxWatermark → GetMetadata_ListFiles → ForEach_File (sequential)
    [per file: GetMetadata_FileModifiedTime → Set_ComputeTemp → Set_CopyTempToMax]
  → Copy_File_Source (filtered by last modified) → Notebook_SilverFills → Notebook_UpdateWatermark
```

**Key design point — file-level watermarking, not row-level.** Copy Data can't filter rows *inside* a CSV by a column value (that only works against queryable sources like the SQL database) — so instead of watermarking on `_ingested_at` as a data column, the watermark tracks **file upload time**, and Copy Data's native "Filter by last modified" setting does the actual filtering. To make this demonstrable without a real continuous data feed, the original 30,240-row `fact_fills_raw.csv` was split chronologically into 3 batch files (`fact_fills_raw_batch1/2/3.csv`, 10,080 rows each) simulating sequential arrivals.

**`pipeline_watermark` table** — one row (`entity_name = 'fact_fills_raw'`, `last_watermark_utc`), seeded at `1900-01-01T00:00:00Z` so the first run treats everything as new.

**Race-condition fix — watermark now reflects actual file time, not "now."** The first version had `Notebook_UpdateWatermark` set the watermark to `current_timestamp()` — simple, but with a real gap: if a new file landed in the folder *during* a run (after `Copy_File_Source` already read the folder listing, but before the notebook set the watermark to "now"), that file's timestamp would end up *older* than the new watermark and get silently, permanently skipped on all future runs. Fixed by computing the watermark from **the actual latest file-modified time among files genuinely seen**, not the clock:
- `Set_InitMaxWatermark` seeds a running-max variable (`maxFileWatermark`) with the old watermark value
- `GetMetadata_ListFiles` + `ForEach_File` (sequential, since a shared variable is being updated) walks every file in the folder, checking each one's actual Last Modified time and keeping the larger of "what we've seen so far" vs. "this file"
- `Notebook_UpdateWatermark` now takes the computed max as a Base parameter (`new_watermark_value`) rather than generating its own timestamp

**Real bug hit: Set Variable activities cannot self-reference.** Confirmed via Microsoft's own docs: <cite index="180-1">in a Set Variable activity, you can't reference the variable being set in the value field — the documented workaround is a temporary variable and a second Set Variable activity.</cite> The natural "compare new value to current running max, update if bigger" expression referenced `maxFileWatermark` while also setting it, which isn't allowed. Fixed per the official pattern: `Set_ComputeTemp` writes the comparison result into a *different* variable (`tempMaxWatermark`), then `Set_CopyTempToMax` copies that into `maxFileWatermark` — three activities per loop iteration instead of two, but genuinely correct.

**Real bug hit: Lookup activity can't run a custom query against a Lakehouse table at all** — confirmed via multiple Fabric community threads: <cite index="169-1">the Query option is only available in Copy Data when using the Lakehouse SQL analytics endpoint; Lookup can reference only Tables or Files</cite> when connected directly to a Lakehouse. Workaround used here: since `pipeline_watermark` only ever has one row, Lookup in plain **Table** mode with "First row only" gets the same result a filtered query would have — no query needed for a single-row table. (If this table ever grows to multiple entities' watermarks, the documented fix is connecting via the Lakehouse's SQL analytics endpoint as an "External"/Azure SQL Database-style connection instead, which does support Query mode.)

**Event-driven trigger — working.** Added a Storage event trigger (OneLake events, Preview) on `pl_fills_incremental`, filtered to `Files/raw/fact_fills_raw/`, backed by a Fabric Activator/Reflex rule (`rule_fills_file_arrival`) saved as its own workspace item. **Confirmed working**: uploading `fact_fills_raw_batch3.csv` triggered an automatic pipeline run with zero manual intervention. First attempt didn't fire (cause not conclusively identified — possibly propagation delay, possibly a rule/filter misconfiguration on the first try); a full reset (dropped table, reset watermark to seed value, cleared the folder, recreated the rule) and retry succeeded.

**Cleanup**: the now-dead `file_incremental` case was removed from `pl_master_orchestrator`'s `Switch_SourceType` (down to 4 cases: `sql_full`, `sql_scd2`, `shortcut_shortcut`, `api_api_paginated`), since `fact_fills_raw` never reaches that Switch anymore with `is_active = false`.

## The pipeline (`pl_master_orchestrator`) — built and verified

Note on naming: the actual workspace uses `Bronze_LH` and `Silver_LH` as Lakehouse names (schema-enabled — tables live under `dbo`), not the `lh_bronze`/`lh_silver` placeholders used in the original guide.

```
Lookup_GetConfig (reads pipeline_config table, full array, 8 rows)
    → Filter_ActiveEntities (is_active = 1, filters 8 rows → 6, now excluding both streaming_pos_events AND fact_fills_raw)
        → ForEach_Entity (parallel, batch=4)
            → Switch_SourceType, on @concat(item().source_type, '_', item().load_type):
                - sql_full            → Copy_SQL_Dimension → Notebook_SilverGeneric
                - sql_scd2            → Copy_SQL_Dimension → Notebook_SilverPatientsSCD2
                - shortcut_shortcut   → GetMetadata_ShortcutCheck → Notebook_SilverDrugs
                - api_api_paginated   → Notebook_DrugLabelAPI → Notebook_SilverDrugLabels
```

**Important design change from the original guide:** the plan called for a nested `Switch_LoadType` inside the `sql` case (to route `dim_patients` to SCD2 vs. the other three dims to generic cleanup). This turned out to be **impossible to build** — Fabric enforces a documented platform restriction: *"If and Switch can't be used inside If and Switch activities."* Confirmed via Microsoft's own docs. Fixed by flattening into a single Switch with a **combined-key expression** (`source_type` + `load_type` concatenated). Originally 5 cases; now 4, after `file_incremental` was removed entirely once `fact_fills_raw` got extracted into its own dedicated pipeline (see above).

**Status: full pipeline confirmed working end to end — bronze → silver → gold, now 6 entities (fact_fills_raw handled separately), single trigger.**

## The gold layer (`nb_gold_build_star_schema` + `pl_gold_layer`)

Builds `fact_pharmacy_fills` (fills joined to drug attributes), `dim_date`, and copies of all silver dimensions into `Gold_LH`. Runs from a **separate child pipeline** (`pl_gold_layer`, containing just one Notebook activity) rather than being a notebook call directly on the master pipeline's canvas — invoked via an **Invoke Pipeline** activity (`Invoke_GoldLayer`) added to `pl_master_orchestrator`'s top-level canvas, connected with a success arrow from `ForEach_Entity` — deliberately outside and after the ForEach, so gold builds exactly once per run, never once per entity, avoiding the same race-condition risk called out for cross-entity silver reads.

**Invoke Pipeline requires a Connection object** (confirmed via Microsoft's docs) — a one-time setup storing your identity in Fabric's credential store, created with **Organizational account** authentication. Not a bug, just an unavoidable one-time prerequisite the first time this activity type is used.

`drug_label_enrichment` is carried into gold as its own standalone table rather than joined into the fact table — the synthetic `dim_drugs` NDC values don't literally match real NDC codes from the live openFDA API, so it stays a separate reference table for its own report tile, a known and documented limitation rather than an oversight.

**Quarantined rows** (`fact_fills_quarantine`, ~15 rows with orphan `prescriber_id`s) live in `Silver_LH`, not `Gold_LH` — deliberately not carried forward, since gold is meant to be the clean, reporting-ready layer. Worth citing the actual count in the README as evidence of the pipeline's validation working, not just claiming the capability abstractly.

## Eventstream (`es_pos_adjudication`) — built and verified

Handles `streaming_pos_events`, kept deliberately outside `pl_master_orchestrator` (bounded batch sources and an unbounded stream don't share a trigger cleanly — same reasoning as originally planned).

**Components:**
- **`pos_producer`** — a Custom App / custom endpoint source added to the Eventstream. This is just an authenticated entry point (a "door" with keys) — it holds no data itself and does nothing without something sending to it.
- **`eventstream_producer.py`** — a notebook script standing in for a real POS system. Reads the 500-event `streaming_pos_events.jsonl` sample from the Lakehouse Files path and sends each event individually over HTTPS, 0.5 seconds apart, authenticated using a SAS token.
- **Destination**: `Bronze_LH.dbo.streaming_pos_events`, input format JSON.

**Real bug hit and routed around:** the originally-planned `azure-eventhub` SDK approach failed with `could not find a version that satisfies the requirement` — likely the same category of org-level restriction seen elsewhere in this project (a curated/restricted package mirror rather than open PyPI access), though not confirmed with certainty. Routed around it entirely by rewriting the producer to use plain HTTPS + a hand-rolled SAS token (stdlib `hmac`/`hashlib`/`base64` + the already-proven `requests` library) instead of the SDK — no package install required at all. Verified working: a full 500-event send completed successfully.

**Also learned along the way:**
- **File API path vs. abfss path**: the producer script uses plain Python `open()`, not Spark — so it needs the locally-mounted File API path (`/lakehouse/default/Files/...`), not an `abfss://` URI, which only Spark/notebookutils APIs understand.
- **Event Hub retention/backlog behavior**: events sent before a destination was connected weren't lost — they sat buffered in the underlying Event Hub, then flowed through immediately once the Lakehouse destination was wired up and published. Explains why data appeared "instantly" the moment the destination connection was drawn.
- **Incoming vs. outgoing message count mismatch** in Data insights is a known, documented pattern (confirmed via Microsoft's docs and community reports) — usually explained by backlog catch-up and/or multiple consumers (e.g. the Live view preview) reading the same events independently. Not a sign of data loss or duplication in the actual destination table; verify via the real row count instead of the metrics graph.
- **Honest framing for the README/demo**: this setup proves the *mechanism* (authenticated real-time ingestion, buffering, routing into a Lakehouse table) genuinely works — it is not simulating production *scale* or *concurrency*. A real deployment would have continuous, automatic event generation from the actual source system, batched sends, and partitioning by something like `pharmacy_id` for ordering guarantees — none of which this demo needed at 500 events sent once.

## Real bugs hit and fixed (good interview material — each is a genuine, specific debugging story)

1. **`is_active` type mismatch** — Filter condition compared string `'true'` against an actual `0/1`-typed column from the loaded table; found via checking the raw Lookup output.
2. **Stale/cached debug output** — a deliberately-wrong test condition (`'definitely_wrong'`) still returned all 8 rows, revealing the output panel was showing a different activity's cached result, not a logic bug.
3. **Table name dynamic content unavailable on Lakehouse "Tables" root** — a known Fabric UI limitation (confirmed via the Fabric community forum); fixed using the "Enter manually" option instead of the usual dynamic-content toggle.
4. **SQL analytics endpoint is read-only** — `DELETE FROM ...` failed with "DML is not supported for this table type"; fixed by running `spark.sql("DROP TABLE ...")` from a notebook instead.
5. **Redundant `Files/` prefix** — control table stored `Files/raw/fact_fills_raw/`, but the Copy activity's Root folder was already set to `Files`, so the actual resolved path became `Files/Files/raw/...` — nonexistent. Fixed by storing paths relative to the selected root folder.
6. **Notebook not attached to a Lakehouse** — `saveAsTable()` failed with `UnsupportedOperationException: No default context found`; fixed by explicitly attaching `Bronze_LH` as the notebook's default Lakehouse via the notebook's Explorer pane.
7. **Cross-lakehouse schema qualification** — `Bronze_LH` is schema-enabled (`dbo` schema), so referencing its tables from a notebook where `Silver_LH` is default requires the full `Bronze_LH.dbo.table` path, not just `Bronze_LH.table`. Whichever Lakehouse is default must be referenced unqualified; only the non-default attached one needs full qualification. Got this backwards twice (once each direction) before landing on the correct pattern.
8. **`_ingested_at` assumption in the generic silver template** — `nb_silver_generic` assumed every bronze table had an `_ingested_at` column to window over for "most recent wins" dedup logic. SQL-copied dimensions never had this column at all (only `fact_fills_raw` and `drug_label_enrichment` do, added explicitly during their own bronze loads). Fixed by dropping the windowing logic entirely for SQL-sourced dims — a plain dedupe on primary key is sufficient anyway, since `Copy_SQL_Dimension` runs in Overwrite mode.
9. **SparkUpgradeException on valid timestamps** — `try_to_timestamp` still threw `SparkUpgradeException` on certain valid-looking ISO timestamps due to Spark's default `EXCEPTION` legacy-parser policy — a stricter safety check than a normal parse failure. Fixed with `spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")`. Notably: this passed all 9 local pytest tests and only surfaced against the real Fabric Spark cluster — a good example of a bug class unit tests alone won't catch, since it depends on session config, not transformation logic.
10. **Switch cannot nest inside Switch** — a genuine Fabric platform restriction (confirmed via Microsoft docs), not a bug per se, but discovered the hard way after building most of the `sql` case around a nested `Switch_LoadType` that turned out to be structurally impossible. Required redesigning `Switch_SourceType` into 5 combined-key cases instead of 4 with a nested switch.
11. **Local module imports don't work in Fabric notebooks** — `from silver_transformations import ...` and `from scd2_helpers import ...` both failed, since a locally-written Python module isn't automatically available inside a Fabric notebook's environment without an extra upload/sys.path step. Fixed by inlining both modules' functions directly into their respective pipeline notebooks (`nb_silver_fills`, `nb_silver_patients_scd2`) — the standalone tested versions still exist and were what the pytest suite actually ran against.
12. **Invoke Pipeline requires a one-time Connection object** — not a bug, but not obvious going in either: the activity won't let you select a target pipeline until a Connection (storing your identity in Fabric's credential store) is created first, via Organizational account, service principal, or workspace identity auth. Created once, reusable for every future Invoke Pipeline activity.
13. **`azure-eventhub` package install failure** — `%pip install azure-eventhub` failed with "could not find a version that satisfies the requirement," while `nest_asyncio` installed fine in the same session — suggests a curated/restricted package source rather than a genuine version-compatibility issue, though the exact cause wasn't confirmed. Routed around it by rewriting the Eventstream producer to use plain HTTPS with a hand-rolled SAS token instead of the SDK, needing only `requests` and Python's standard library.
14. **Eventstream schema association needs live data first** — attempting to activate schema association on the `pos_producer` source immediately after creating it failed with "No schema is mapped to this data source," since there was no data flowing yet to infer a schema from. Same ordering lesson as several bugs above: activate schema association only after real events have flowed through at least once.
15. **Eventstream Details/Keys pane only exists in Live view, after Publish** — in Edit mode, clicking a source node only shows a rename sidebar and an "Authoring errors" tab; the full Details pane with Basic/Keys/Kafka tabs only appears after publishing and switching to Live view.
16. **Lookup activity cannot run a custom query against a Lakehouse table** — confirmed via multiple Fabric community threads: Query mode is only available in Copy Data via the Lakehouse SQL analytics endpoint, never in Lookup connected directly to a Lakehouse. Worked around by exploiting the fact that `pipeline_watermark` only has one row — plain Table mode + "First row only" gets the same result as a filtered query would have, no query needed.
17. **Stale leftover data from earlier test runs inflated a row count** — `fact_fills_raw` showed 70,560 rows instead of the expected 10,080 after the first incremental run, caused by the original unsplit 30,240-row CSV still sitting in the same folder as the new batch files, plus accumulated duplicate appends from testing the pipeline before watermark logic existed. Fixed with a full reset: drop the table, clear the folder to only the intended batch file(s), reset the watermark to its seed value.
18. **Set Variable activities cannot self-reference** — a "compare to current value, keep the bigger one" expression referencing the same variable it was setting failed with "the expression has self referencing variable." Confirmed as a documented, deliberate platform restriction (not a bug). Fixed per Microsoft's own documented pattern: compute the result into a separate temporary variable first, then copy that into the real variable with a second, non-comparing Set Variable activity.
19. **Silent STRING-typed currency columns, across three layers** — see the dedicated section above. `overwriteSchema=true` needed on every write in the affected chain, not just the first one where the type actually changed.
20. **Org-level access restriction on the Power BI service specifically** — signing in returned "Your sign-in was successful but you don't have permission to access this resource," distinct from (and in addition to) the Azure resource, Git integration, and possible PyPI restrictions hit earlier in this project. Fabric's own workspace/capacity access, including the semantic model editor itself, continued working fine — this restriction is scoped to the separate Power BI service layer specifically. Resulted in a deliberate decision to drop full report-building from scope (see the semantic model section above) rather than keep fighting an access wall for a layer that's arguably outside core data engineering anyway.
21. **`fact_fills_quarantine` was silently accumulating duplicates on every run** — a real correctness bug independent of the type-mismatch issue. `quarantine.write` used `mode("append")`, but `split_quarantine()` recomputes the *entire* quarantine set fresh every single run, not an incremental addition — meaning every run was re-appending the same already-quarantined rows on top of themselves, growing without bound. Fixed by switching to `mode("overwrite")` with `overwriteSchema=true`, which also happened to fix a recurring Delta schema-merge conflict on `days_supply` at the same time.
22. **Multi-dependency activities are a logical AND, and "Completed" excludes Skipped** — see the error handling section above. A design that seemed reasonable (Notify depending on both mutually-exclusive log activities) would have permanently deadlocked; confirmed via Microsoft's documentation before building it, not discovered by trial and error this time.

## Fully built and wired into the pipeline (as of today)

- **All 7 entities**, bronze layer, all 5 source-type mechanics
- **All 7 entities**, silver layer, chained immediately after their bronze step within the same `Switch_SourceType` branch:
  - `nb_silver_generic` (dim_prescribers, dim_pharmacies, dim_plans)
  - `nb_silver_patients_scd2` (dim_patients — SCD Type 2, tracks `plan_id`/`state`)
  - `nb_silver_drugs` (dim_drugs, shortcut-backed)
  - `nb_silver_drug_labels` (drug_label_enrichment)
  - `nb_silver_fills` (fact_fills_raw — dedupe, date standardization, days_supply fix, prescriber-orphan quarantine)
- **Gold layer**, built once per run via `Invoke_GoldLayer` → `pl_gold_layer` → `nb_gold_build_star_schema`, after `ForEach_Entity` fully completes
- **Design rule enforced throughout:** cross-entity joins inside a silver notebook (e.g. fills joining to drugs/prescribers) always read from `Bronze_LH`, never from another entity's `Silver_LH` output — avoids race conditions given `ForEach_Entity` runs entities in parallel with no guaranteed ordering between them. The gold notebook is the one place where reading another entity's *silver* output is safe, precisely because it only runs after the whole ForEach is done.
- **Eventstream** for `streaming_pos_events` — source, destination, and producer script all verified working end to end, 500 events confirmed landed in `Bronze_LH.dbo.streaming_pos_events`.
- **`pl_fills_incremental`** — dedicated pipeline with genuine watermark-based incremental file loading, a fixed race-condition gap (watermark now reflects true file-modified time rather than pipeline-completion time), and a working, confirmed event-driven trigger (uploads to `Files/raw/fact_fills_raw/` fire the pipeline automatically, no manual run needed).
- **Semantic model** (`Pharmacy_Analytics_SM`) — Direct Lake, 8 tables, 6 relationships, 4 measures, verified working. See `semantic-model-design.md` for full detail.
- **Error handling, logging, and Notify** — built on `pl_fills_incremental` as a reference pattern (see dedicated section above). Deliberately not replicated to `pl_master_orchestrator` — a documented scope decision.
- **Dataflow Gen2** (`df_drug_reference_mart`) — built, verified mechanically working; the specific drug-reference blend produced no matches, documented as a data limitation rather than a tool failure.
- **Dynamic Data Masking** on `pharmacy_opsdb.dbo.patients` — applied and verified via `sys.masked_columns` metadata (see dedicated section above).

## Not yet built

- **Power BI report pages, RLS** — explicitly out of scope now, per the org access restriction and the DE-vs-BI scope reasoning above.
- **CI/CD deployment pipeline, data quality checks** — still just documented/discussed, not started.
- **Error handling/logging/Notify on `pl_master_orchestrator`** — deliberately not built; `pl_fills_incremental` stands as the reference implementation of a directly reusable pattern.

## Not pursued (explicit decision, not an oversight)

- **Kafka** for `streaming_pos_events` — prototyped (producer script, docker-compose, Confluent Cloud guide), then explicitly reverted back to the simpler Eventstream sample-data approach per instruction. Kafka files remain in `/streaming/` if revisited later.
- **`azure-eventhub` SDK** for the Eventstream producer — blocked by the package install issue above; the REST/HTTPS + SAS token version is the one actually in use, not a temporary workaround pending a fix.
- **Row-level watermarking** for `fact_fills_raw` — not possible with a flat-file Copy Data source; file-level (last-modified) watermarking was used instead, which is the standard real-world pattern for file-based incremental loads anyway.
- **Full Power BI report/RLS** — see the semantic model section above; blocked by org access restriction, and arguably outside core DE scope regardless.
- **Error handling/logging replicated across every pipeline branch** — built once as a reference pattern on `pl_fills_incremental`; deliberately not copied to the other 6 entities in `pl_master_orchestrator`.
- **Live behavioral test of Dynamic Data Masking** and **governance sensitivity labels beyond the attempt made** — both hit genuine platform/tenant-configuration walls, documented in their dedicated sections above rather than repeated here.

## Next steps — packaging only, no further building planned

1. Assemble the demo video from captured clips: pipeline run with parallel entities, a quarantined-row catch, incremental-load count growth, the file-arrival trigger firing automatically, Eventstream throughput live, the semantic model's relationship diagram, and the closing `Gold_LH` opioid-utilization query.
2. Manually assemble the GitHub repo — Git integration remains blocked, so this means: notebook exports (`.ipynb` and/or the `.py` files already delivered throughout this project), the dataset and control tables, `PROJECT_STATUS.md`, `semantic-model-design.md`, the README, and screenshots of every canvas (pipelines, semantic model, Eventstream, Activator rule, DDM metadata query) as supporting evidence for what can't be exported as a literal file.
3. Final README pass — update for the dropped Power BI scope, the semantic model, the error-handling/logging layer, DDM/governance tag outcomes, and the full 22-item bug list, once the above are in place.
4. If CI/CD is ever unblocked (pending the Git integration question with the tenant admin), it remains the one item with no current workaround — everything else in this project has either been built, or has a documented, deliberate reason it wasn't.
