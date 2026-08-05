# Project status: Pharmacy fills analytics platform

A running record of what's built, what's working, and what's left. This is the final build entry. DDM has been applied and verified. Sensitivity labels were attempted and found unavailable in this tenant's configuration, documented below. All planned net-new building is complete. What remains is packaging (video, repo assembly, README finalization) and a few items that are explicitly out of scope.

---

## Goal

A portfolio-grade Microsoft Fabric data engineering project demonstrating a metadata-driven pipeline that pulls from multiple genuinely different source types. It's not a single-source tutorial replica.

## Architecture

The overall pattern is Bronze to Silver to Gold to semantic model (not to a Power BI report; see the scope note below). Bronze pulls from five different source mechanics, each handled differently.

| Entity | Source type | Mechanism |
|---|---|---|
| `dim_prescribers`, `dim_pharmacies`, `dim_plans` | SQL | Copy Data from `pharmacy_opsdb` (Fabric-native SQL database) |
| `dim_patients` | SQL | Same source, routed to SCD Type 2 historization instead of overwrite |
| `dim_drugs` | Shortcut | Zero-copy OneLake shortcut to `Formulary_Reference_LH` in workspace `Shared_Reference_Data` |
| `drug_label_enrichment` | API | Paginated openFDA calls, handled inside a notebook |
| `fact_fills_raw` | File | CSV drop, Copy Data, incremental load type |
| `streaming_pos_events` | Eventstream | Outside the batch pipeline entirely |

## Semantic model: built and verified, report-building out of scope

`Pharmacy_Analytics_SM` was built on `Gold_LH` in Direct Lake mode. It has 8 tables, 6 relationships forming a proper star schema, and 4 DAX measures, all verified working. A `Total Fills` breakdown sliced by `dim_drug[drug_class]` came back real, non-blank, and correctly filtered. Full details, including the relationship table, measure DAX, and a real type bug found and fixed along the way, are in `semantic-model-design.md`.

Report pages and RLS were not built. A verified, relationship-correct, Direct Lake semantic model is the appropriate stopping point for a data engineering portfolio, since report and dashboard building is data analyst and BI developer territory. The closing proof moment for the demo video is a live query against `Gold_LH` (the opioid utilization review) rather than a report screenshot.

## Error handling, logging, and notification: built and verified on pl_fills_incremental, deliberately not replicated elsewhere

This was built as a reference implementation on one pipeline, then deliberately left there rather than copied across `pl_master_orchestrator`'s 6 entities. That's a scope decision, documented explicitly rather than left implicit.

`pipeline_run_log` lives in `Bronze_LH`, with one row per run: `run_id`, `pipeline_name`, `entity_name`, `status`, `rows_processed`, `error_message`, `run_timestamp`. It was created via DDL in a one-time setup notebook (`nb_setup_run_log`), not the CSV-seed pattern used for `pipeline_config` and `pipeline_watermark`.

The mechanism: `nb_silver_fills` returns a structured result via `mssparkutils.notebook.exit(json.dumps({...}))`, readable downstream as `@activity('Notebook_SilverFills').output.result.exitValue`. Three independent activities branch off `Notebook_SilverFills`, each with its own dependency condition.

- `Notebook_LogRun_Success` (Succeeded) reads the real exit value and logs `rows_processed`.
- `Notebook_LogRun_Failure` (Failed) logs a static message pointing to Monitoring hub for detail, rather than fighting a validator restriction on referencing `.Error` from a Failed-dependency activity.
- `Notify` (Completed) is an Office 365 Outlook activity, which required a one-time Connection object, the same pattern as Invoke Pipeline's connection requirement.

There's a real design mistake that got caught and corrected before building the wrong thing. The original instinct was to chain `Notify` after both log activities, so notification happens once logging is done. Checking against Microsoft's own documentation showed this doesn't work: multiple incoming dependencies are evaluated as a logical AND, and "Completed" specifically means Succeeded or Failed. It does not include Skipped. Since exactly one of the two log activities is always Skipped, since they're mutually exclusive, a Notify depending on both via Completed would permanently deadlock, because the skipped one could never satisfy that condition. The fix was connecting `Notify` directly and independently off `Notebook_SilverFills`, sidestepping the multi-dependency AND entirely.

For testing, several small incremental batch files (batch4 through batch10, 20 to 1000 rows each) were generated specifically to re-trigger `pl_fills_incremental` repeatedly without reusing already-processed data. This confirmed the log table and Notify both fire correctly across several runs. The `Notify` connection was initially missing from the canvas entirely on the first test. An activity with no incoming connection simply never runs, and it doesn't fail the pipeline either, since it's silently skipped. This was caught by noticing Monitoring hub's activity list didn't include it at all, not by an error message.

## Dataflow Gen2: built, demonstrates the tool correctly, the specific blend produced no matches

`df_drug_reference_mart` merges `dim_drugs` (`Silver_LH`) with `drug_label_enrichment` (`Silver_LH`) through Power Query's Merge queries UI, writing to `Gold_LH.dbo.dim_drug_reference_mart`. It was attempted first on `ndc`, which predictably came back all-null given the known synthetic-versus-real NDC mismatch documented since the gold layer was built. It was then retried on normalized (lowercase, trimmed) `generic_name`, which also produced no matches, most likely due to formatting and naming differences between the synthetic dataset's simplified drug names and openFDA's real-world naming conventions. This is documented honestly as a data-characteristic limitation, not a tool failure. The actual point, demonstrating Dataflow Gen2's merge, transform, and destination mechanics, along with the judgment of knowing when to reach for low-code versus code, stands regardless of match rate.

## A real bug worth highlighting: silent type mismatch across three layers

`plan_paid_amount` and four sibling currency columns were stored as STRING, not numeric, all the way from bronze through gold. This passed silently through every layer with no error, only surfacing when the semantic model's `SUM()` measure tried to do real arithmetic on the column, failing with "The function SUM cannot work with values of type String." The fix was adding explicit `DecimalType(10,2)` casts in `nb_silver_fills`, which then required adding `overwriteSchema=true` to every downstream write in the chain (`fact_fills`, `fact_fills_quarantine`, and every table `nb_gold_build_star_schema` writes). Delta's `mergeSchema` only allows adding new columns, not changing an existing column's type, so each table in the chain needed the same fix individually. This is a good concrete example of "runs without error" not meaning "correct," the same lesson as the ANSI-mode date bug, just surfacing in a different layer this time.

## Dynamic Data Masking: applied and verified on pharmacy_opsdb.dbo.patients

```sql
ALTER TABLE dbo.patients ALTER COLUMN first_name ADD MASKED WITH (FUNCTION = 'partial(1, "XXXXXXX", 0)');
ALTER TABLE dbo.patients ALTER COLUMN last_name ADD MASKED WITH (FUNCTION = 'partial(1, "XXXXXXX", 0)');
ALTER TABLE dbo.patients ALTER COLUMN date_of_birth ADD MASKED WITH (FUNCTION = 'default()');
```

This was verified via `sys.masked_columns` metadata (screenshot captured), with all three columns confirmed correctly configured.

Live behavioral testing, querying as a genuinely non-privileged user, wasn't completed. Fabric SQL database only supports Microsoft Entra ID authentication, with no SQL-auth logins at all, so `CREATE USER ... WITHOUT LOGIN`, the standard SQL Server pattern for a lightweight test principal, isn't supported. Any test user must be a real Entra identity via `CREATE USER [email] FROM EXTERNAL PROVIDER`, and no second account was available to test with. The metadata verification is treated as sufficient proof, since it's the same source of truth Fabric itself uses to enforce masking, not merely a claim that the T-SQL executed without error.

This wasn't extended to the Lakehouse SQL analytics endpoint. It was applied at the true source (`pharmacy_opsdb`) only, per the original plan's priority. The reminder that DDM only protects SQL query access, not Spark or notebook reads of the same underlying data, remains the key caveat to state alongside this in the README.

## Governance tags: attempted, unavailable in this tenant

Right-clicking `pharmacy_opsdb` for Apply sensitivity label showed no label options at all. This means the tenant has no Purview sensitivity label taxonomy configured, a separate admin-level setup distinct from the item-level permission restrictions hit throughout this project (Azure resources, Git integration, Power BI service access). It's not something fixable from within the workspace. It would require a tenant admin configuring Purview Information Protection first. This is documented as attempted but blocked rather than skipped. The correct native mechanism, Apply sensitivity label at the item level, was identified and tried. It's the taxonomy itself that doesn't exist here.

---

# Project complete: remaining items are packaging or explicitly out of scope

All planned net-new building is done. What's left:

- Packaging: video assembly from captured clips, manual GitHub repo assembly (Git integration blocked), and a final README pass.
- Explicitly out of scope: full Power BI report and RLS (org access restriction), CI/CD (Git integration blocked), error handling and logging replicated beyond the one reference pipeline (deliberate scope decision), and governance tags beyond the attempt documented above (tenant configuration gap, not fixable from the workspace).

---

## Git integration: not actually available

Contrary to earlier assumptions in this project's guide, Git integration was never enabled. It's blocked on this account, likely by the same kind of org-level restriction that blocked Azure resource creation, though it could also be a separate tenant-level toggle specifically for Git integration. This means everything built has no version history and no repo backup through Fabric's native mechanism.

As a current substitute, every notebook and the control table exists as a delivered file from the conversation where it was built. These serve as the de facto version history until this is resolved. Re-export and re-save the current state after any significant change.

To actually fix this, ask whoever administers the Fabric tenant specifically about the Git integration setting, which is distinct from general Azure resource permissions. It may be a narrower ask than the Azure SQL Database restriction was.

## Why Fabric-native SQL database instead of Azure SQL Database

The original plan used Azure SQL Database. The project pivoted to a native SQL database in Fabric item after hitting an Azure resource-creation restriction ("I don't have permission to access this resource"). The Fabric-native option needs only workspace permissions already in hand, not Azure Resource Manager access.

## The dataset

The dataset is fully synthetic, generated with Faker using a fixed seed, and reproducible via `generate_pharmacy_data.py`. It includes 600 patients, 150 prescribers, 45 pharmacies, 8 plans, 77 drug and strength rows, about 30,240 pharmacy fills, and a 500-row streaming sample.

`fact_fills_raw` is deliberately messy on purpose. It has 4 inconsistent date formats, mixed-case drug names, about 0.8 percent duplicate claims, some nulls, 15 orphan prescriber IDs, and occasional negative `days_supply` typos, so the silver layer has real cleansing work to demonstrate instead of synthetic-clean data.

## The control table (dim_pipeline_config.csv)

This is the single file driving the whole pipeline. It was edited several times as the design evolved.

1. Added SQL sourcing for four dimensions, from an original all-file design.
2. Added the `dim_drugs` shortcut. This started as an ADLS Gen2 path, was corrected to a OneLake cross-workspace path after switching approach, then corrected again after the workspace was renamed `Shared_Reference_Data`.
3. Added `drug_label_enrichment` as a new API-sourced row.
4. Changed `dim_patients` to `load_type = scd2`.
5. `streaming_pos_events` was briefly set to `source_type = kafka`, then reverted back to `eventstream`.
6. Fixed a real bug: `fact_fills_raw`'s path included a redundant `Files/` prefix, causing "File Not Found" once the Copy activity's Root folder was already set to `Files`.
7. `fact_fills_raw` was set to `is_active = false` and extracted out of the master orchestrator entirely into its own dedicated pipeline (see below). This follows the same pattern already used for `streaming_pos_events`, just for a different underlying reason: dedicated incremental and trigger logic rather than a bounded versus unbounded mismatch.

## pl_fills_incremental: dedicated pipeline with true watermark-based incremental loading

This was extracted out of `pl_master_orchestrator`'s `file_incremental` Switch case entirely, so a fills-file arrival doesn't re-run all 7 entities just to process one new file. This is real incremental loading, not just Append. The pipeline only processes files uploaded since the last successful run, verified across a genuine multi-run test: 10,080, then 20,160, then 30,240 rows across 3 separate runs, each only picking up its one new batch.

```
Lookup_GetWatermark -> Set_InitMaxWatermark -> GetMetadata_ListFiles -> ForEach_File (sequential)
    [per file: GetMetadata_FileModifiedTime -> Set_ComputeTemp -> Set_CopyTempToMax]
  -> Copy_File_Source (filtered by last modified) -> Notebook_SilverFills -> Notebook_UpdateWatermark
```

The key design point is file-level watermarking, not row-level. Copy Data can't filter rows inside a CSV by a column value. That only works against queryable sources like the SQL database. So instead of watermarking on `_ingested_at` as a data column, the watermark tracks file upload time, and Copy Data's native "Filter by last modified" setting does the actual filtering. To make this demonstrable without a real continuous data feed, the original 30,240-row `fact_fills_raw.csv` was split chronologically into 3 batch files (`fact_fills_raw_batch1/2/3.csv`, 10,080 rows each), simulating sequential arrivals.

The `pipeline_watermark` table has one row (`entity_name = 'fact_fills_raw'`, `last_watermark_utc`), seeded at `1900-01-01T00:00:00Z` so the first run treats everything as new.

There's a race-condition fix worth detailing: the watermark now reflects actual file time, not "now." The first version had `Notebook_UpdateWatermark` set the watermark to `current_timestamp()`. That's simple, but it has a real gap. If a new file landed in the folder during a run, after `Copy_File_Source` already read the folder listing but before the notebook set the watermark to "now," that file's timestamp would end up older than the new watermark and get silently, permanently skipped on all future runs. The fix was to compute the watermark from the actual latest file-modified time among files genuinely seen, not the clock.

- `Set_InitMaxWatermark` seeds a running-max variable (`maxFileWatermark`) with the old watermark value.
- `GetMetadata_ListFiles` and `ForEach_File`, run sequentially since a shared variable is being updated, walk every file in the folder, checking each one's actual Last Modified time and keeping the larger of what's been seen so far versus this file.
- `Notebook_UpdateWatermark` now takes the computed max as a Base parameter (`new_watermark_value`) rather than generating its own timestamp.

A real bug came up here: Set Variable activities cannot self-reference. This was confirmed via Microsoft's own documentation, which states that in a Set Variable activity, you can't reference the variable being set in the value field. The documented workaround is a temporary variable and a second Set Variable activity. The natural "compare new value to current running max, update if bigger" expression referenced `maxFileWatermark` while also setting it, which isn't allowed. The fix, following the official pattern, was having `Set_ComputeTemp` write the comparison result into a different variable (`tempMaxWatermark`), then `Set_CopyTempToMax` copies that into `maxFileWatermark`. That's three activities per loop iteration instead of two, but it's genuinely correct.

Another real bug: the Lookup activity can't run a custom query against a Lakehouse table at all. This was confirmed via multiple Fabric community threads, which state that the Query option is only available in Copy Data when using the Lakehouse SQL analytics endpoint, and that Lookup can reference only Tables or Files when connected directly to a Lakehouse. The workaround here relies on the fact that `pipeline_watermark` only ever has one row, so Lookup in plain Table mode with "First row only" gets the same result a filtered query would have, with no query needed for a single-row table. If this table ever grows to cover multiple entities' watermarks, the documented fix is connecting via the Lakehouse's SQL analytics endpoint as an external, Azure SQL Database-style connection instead, which does support Query mode.

The event-driven trigger is working. A Storage event trigger (OneLake events, Preview) was added on `pl_fills_incremental`, filtered to `Files/raw/fact_fills_raw/`, backed by a Fabric Activator (Reflex) rule (`rule_fills_file_arrival`) saved as its own workspace item. This was confirmed working: uploading `fact_fills_raw_batch3.csv` triggered an automatic pipeline run with zero manual intervention. The first attempt didn't fire, and the cause wasn't conclusively identified. It was possibly a propagation delay, possibly a rule or filter misconfiguration on the first try. A full reset (dropping the table, resetting the watermark to its seed value, clearing the folder, and recreating the rule) and a retry succeeded.

For cleanup, the now-dead `file_incremental` case was removed from `pl_master_orchestrator`'s `Switch_SourceType`, bringing it down to 4 cases (`sql_full`, `sql_scd2`, `shortcut_shortcut`, `api_api_paginated`), since `fact_fills_raw` never reaches that Switch anymore with `is_active = false`.

## The pipeline (pl_master_orchestrator): built and verified

A note on naming: the actual workspace uses `Bronze_LH` and `Silver_LH` as Lakehouse names, schema-enabled, with tables living under `dbo`, not the `lh_bronze` and `lh_silver` placeholders used in the original guide.

```
Lookup_GetConfig (reads pipeline_config table, full array, 8 rows)
    -> Filter_ActiveEntities (is_active = 1, filters 8 rows to 6, now excluding both streaming_pos_events and fact_fills_raw)
        -> ForEach_Entity (parallel, batch=4)
            -> Switch_SourceType, on @concat(item().source_type, '_', item().load_type):
                - sql_full            -> Copy_SQL_Dimension -> Notebook_SilverGeneric
                - sql_scd2            -> Copy_SQL_Dimension -> Notebook_SilverPatientsSCD2
                - shortcut_shortcut   -> GetMetadata_ShortcutCheck -> Notebook_SilverDrugs
                - api_api_paginated   -> Notebook_DrugLabelAPI -> Notebook_SilverDrugLabels
```

There's an important design change from the original guide worth noting. The plan called for a nested `Switch_LoadType` inside the `sql` case, to route `dim_patients` to SCD2 versus the other three dims to generic cleanup. This turned out to be impossible to build. Fabric enforces a documented platform restriction: Switch activities can't be used inside other Switch or If activities. This was confirmed via Microsoft's own documentation. The fix was flattening into a single Switch with a combined-key expression, `source_type` and `load_type` concatenated together. There were originally 5 cases, now down to 4, after `file_incremental` was removed entirely once `fact_fills_raw` got extracted into its own dedicated pipeline.

Status: the full pipeline is confirmed working end to end, bronze through silver through gold, now with 6 entities since `fact_fills_raw` is handled separately, in a single trigger.

## The gold layer (nb_gold_build_star_schema and pl_gold_layer)

This builds `fact_pharmacy_fills` (fills joined to drug attributes), `dim_date`, and copies of all silver dimensions into `Gold_LH`. It runs from a separate child pipeline, `pl_gold_layer`, containing just one Notebook activity, rather than being a notebook call directly on the master pipeline's canvas. It's invoked via an Invoke Pipeline activity (`Invoke_GoldLayer`) added to `pl_master_orchestrator`'s top-level canvas, connected with a success arrow from `ForEach_Entity`, deliberately outside and after the ForEach, so gold builds exactly once per run, never once per entity. This avoids the same race-condition risk called out for cross-entity silver reads.

Invoke Pipeline requires a Connection object, confirmed via Microsoft's documentation. This is a one-time setup storing your identity in Fabric's credential store, created with Organizational account authentication. It's not a bug, just an unavoidable one-time prerequisite the first time this activity type is used.

`drug_label_enrichment` is carried into gold as its own standalone table rather than joined into the fact table, since the synthetic `dim_drugs` NDC values don't literally match real NDC codes from the live openFDA API. It stays a separate reference table for its own report tile, a known and documented limitation rather than an oversight.

Quarantined rows (`fact_fills_quarantine`, about 15 rows with orphan prescriber IDs) live in `Silver_LH`, not `Gold_LH`. They're deliberately not carried forward, since gold is meant to be the clean, reporting-ready layer. It's worth citing the actual count in the README as evidence of the pipeline's validation working, not just claiming the capability abstractly.

## Eventstream (es_pos_adjudication): built and verified

This handles `streaming_pos_events`, kept deliberately outside `pl_master_orchestrator`, since bounded batch sources and an unbounded stream don't share a trigger cleanly. That's the same reasoning as originally planned.

The components:

- `pos_producer` is a Custom App / custom endpoint source added to the Eventstream. It's just an authenticated entry point, like a door with keys. It holds no data itself and does nothing without something sending to it.
- `eventstream_producer.py` is a notebook script standing in for a real POS system. It reads the 500-event `streaming_pos_events.jsonl` sample from the Lakehouse Files path and sends each event individually over HTTPS, 0.5 seconds apart, authenticated using a SAS token.
- The destination is `Bronze_LH.dbo.streaming_pos_events`, with input format JSON.

There's a real bug that was hit and routed around. The originally planned `azure-eventhub` SDK approach failed with "could not find a version that satisfies the requirement," likely the same category of restriction seen elsewhere in this project, possibly a curated or restricted package mirror rather than open PyPI access, though this wasn't confirmed with certainty. It was routed around entirely by rewriting the producer to use plain HTTPS with a hand-rolled SAS token, using the standard library's `hmac`, `hashlib`, and `base64` modules plus the already-proven `requests` library, instead of the SDK. No package install was required at all, and a full 500-event send completed successfully.

A few other things were learned along the way:

- File API path versus abfss path: the producer script uses plain Python `open()`, not Spark, so it needs the locally-mounted File API path (`/lakehouse/default/Files/...`), not an `abfss://` URI, which only Spark and notebookutils APIs understand.
- Event Hub retention and backlog behavior: events sent before a destination was connected weren't lost. They sat buffered in the underlying Event Hub, then flowed through immediately once the Lakehouse destination was wired up and published. This explains why data appeared instantly the moment the destination connection was drawn.
- The incoming versus outgoing message count mismatch in Data insights is a known, documented pattern, confirmed via Microsoft's documentation and community reports. It's usually explained by backlog catch-up and multiple consumers, such as the Live view preview, reading the same events independently. It's not a sign of data loss or duplication in the actual destination table. Verify via the real row count instead of the metrics graph.
- For honest framing in the README and demo: this setup proves the mechanism, meaning authenticated real-time ingestion, buffering, and routing into a Lakehouse table, genuinely works. It is not simulating production scale or concurrency. A real deployment would have continuous, automatic event generation from the actual source system, batched sends, and partitioning by something like `pharmacy_id` for ordering guarantees, none of which this demo needed at 500 events sent once.

## Real bugs hit and fixed (good interview material, each is a genuine, specific debugging story)

1. `is_active` type mismatch. The Filter condition compared string `'true'` against an actual `0/1`-typed column from the loaded table. Found by checking the raw Lookup output.
2. Stale or cached debug output. A deliberately wrong test condition (`'definitely_wrong'`) still returned all 8 rows, revealing the output panel was showing a different activity's cached result, not a logic bug.
3. Table name dynamic content unavailable on the Lakehouse "Tables" root. This is a known Fabric UI limitation, confirmed via the Fabric community forum, and was fixed using the "Enter manually" option instead of the usual dynamic-content toggle.
4. The SQL analytics endpoint is read-only. `DELETE FROM ...` failed with "DML is not supported for this table type," fixed by running `spark.sql("DROP TABLE ...")` from a notebook instead.
5. Redundant `Files/` prefix. The control table stored `Files/raw/fact_fills_raw/`, but the Copy activity's Root folder was already set to `Files`, so the actual resolved path became `Files/Files/raw/...`, which didn't exist. Fixed by storing paths relative to the selected root folder.
6. Notebook not attached to a Lakehouse. `saveAsTable()` failed with `UnsupportedOperationException: No default context found`, fixed by explicitly attaching `Bronze_LH` as the notebook's default Lakehouse via the notebook's Explorer pane.
7. Cross-lakehouse schema qualification. `Bronze_LH` is schema-enabled, so referencing its tables from a notebook where `Silver_LH` is default requires the full `Bronze_LH.dbo.table` path, not just `Bronze_LH.table`. Whichever Lakehouse is default must be referenced unqualified, and only the non-default attached one needs full qualification. This got flipped backwards twice, once in each direction, before landing on the correct pattern.
8. The `_ingested_at` assumption in the generic silver template. `nb_silver_generic` assumed every bronze table had an `_ingested_at` column to window over for "most recent wins" dedup logic. SQL-copied dimensions never had this column at all. Only `fact_fills_raw` and `drug_label_enrichment` do, added explicitly during their own bronze loads. This was fixed by dropping the windowing logic entirely for SQL-sourced dims, since a plain dedupe on primary key is sufficient anyway, given that `Copy_SQL_Dimension` runs in Overwrite mode.
9. A SparkUpgradeException on valid timestamps. `try_to_timestamp` still threw a SparkUpgradeException on certain valid-looking ISO timestamps, due to Spark's default EXCEPTION legacy-parser policy, a stricter safety check than a normal parse failure. This was fixed with `spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")`. Notably, this passed all 9 local pytest tests and only surfaced against the real Fabric Spark cluster, a good example of a bug class that unit tests alone won't catch, since it depends on session config rather than transformation logic.
10. Switch activities cannot nest inside Switch activities. This is a genuine Fabric platform restriction, confirmed via Microsoft's documentation, not a bug exactly, but it was discovered the hard way after building most of the `sql` case around a nested `Switch_LoadType` that turned out to be structurally impossible. It required redesigning `Switch_SourceType` into 4 combined-key cases instead of using a nested switch.
11. Local module imports don't work in Fabric notebooks. `from silver_transformations import ...` and `from scd2_helpers import ...` both failed, since a locally-written Python module isn't automatically available inside a Fabric notebook's environment without an extra upload and sys.path step. This was fixed by inlining both modules' functions directly into their respective pipeline notebooks (`nb_silver_fills`, `nb_silver_patients_scd2`). The standalone tested versions still exist and were what the pytest suite actually ran against.
12. Invoke Pipeline requires a one-time Connection object. Not a bug, but not obvious going in either. The activity won't let you select a target pipeline until a Connection, storing your identity in Fabric's credential store, is created first, via Organizational account, service principal, or workspace identity auth. It's created once and reusable for every future Invoke Pipeline activity.
13. The `azure-eventhub` package install failure. `%pip install azure-eventhub` failed with "could not find a version that satisfies the requirement," while `nest_asyncio` installed fine in the same session. This suggests a curated or restricted package source rather than a genuine version-compatibility issue, though the exact cause wasn't confirmed. It was routed around by rewriting the Eventstream producer to use plain HTTPS with a hand-rolled SAS token instead of the SDK, needing only `requests` and Python's standard library.
14. Eventstream schema association needs live data first. Attempting to activate schema association on the `pos_producer` source immediately after creating it failed with "No schema is mapped to this data source," since there was no data flowing yet to infer a schema from. This is the same ordering lesson as several bugs above: activate schema association only after real events have flowed through at least once.
15. The Eventstream Details and Keys pane only exists in Live view, after Publish. In Edit mode, clicking a source node only shows a rename sidebar and an "Authoring errors" tab. The full Details pane with Basic, Keys, and Kafka tabs only appears after publishing and switching to Live view.
16. The Lookup activity cannot run a custom query against a Lakehouse table. This was confirmed via multiple Fabric community threads: Query mode is only available in Copy Data via the Lakehouse SQL analytics endpoint, never in Lookup connected directly to a Lakehouse. It was worked around by exploiting the fact that `pipeline_watermark` only has one row, so plain Table mode with "First row only" gets the same result as a filtered query would have, with no query needed.
17. Stale leftover data from earlier test runs inflated a row count. `fact_fills_raw` showed 70,560 rows instead of the expected 10,080 after the first incremental run. This was caused by the original unsplit 30,240-row CSV still sitting in the same folder as the new batch files, plus accumulated duplicate appends from testing the pipeline before watermark logic existed. It was fixed with a full reset: drop the table, clear the folder to only the intended batch files, and reset the watermark to its seed value.
18. Set Variable activities cannot self-reference. A "compare to current value, keep the bigger one" expression referencing the same variable it was setting failed with "the expression has self referencing variable." This is a documented, deliberate platform restriction, not a bug. It was fixed per Microsoft's own documented pattern: compute the result into a separate temporary variable first, then copy that into the real variable with a second, non-comparing Set Variable activity.
19. Silent STRING-typed currency columns, across three layers. See the dedicated section above. `overwriteSchema=true` was needed on every write in the affected chain, not just the first one where the type actually changed.
20. An access restriction on the Power BI service specifically. Signing in returned "Your sign-in was successful but you don't have permission to access this resource," distinct from, and in addition to, the Azure resource, Git integration, and possible PyPI restrictions hit earlier in this project. Fabric's own workspace and capacity access, including the semantic model editor itself, continued working fine. This restriction is scoped to the separate Power BI service layer specifically, and it led to a deliberate decision to drop full report-building from scope rather than keep fighting an access wall for a layer that's arguably outside core data engineering anyway.
21. `fact_fills_quarantine` was silently accumulating duplicates on every run. This is a real correctness bug independent of the type-mismatch issue. `quarantine.write` used `mode("append")`, but `split_quarantine()` recomputes the entire quarantine set fresh every single run, not an incremental addition, meaning every run was re-appending the same already-quarantined rows on top of themselves, growing without bound. This was fixed by switching to `mode("overwrite")` with `overwriteSchema=true`, which also happened to fix a recurring Delta schema-merge conflict on `days_supply` at the same time.
22. Multi-dependency activities are a logical AND, and "Completed" excludes Skipped. See the error handling section above. A design that seemed reasonable, Notify depending on both mutually exclusive log activities, would have permanently deadlocked. This was confirmed via Microsoft's documentation before building it, not discovered by trial and error this time.

## Fully built and wired into the pipeline

- All 7 entities, bronze layer, all 5 source-type mechanics.
- All 7 entities, silver layer, chained immediately after their bronze step within the same `Switch_SourceType` branch: `nb_silver_generic` for `dim_prescribers`, `dim_pharmacies`, and `dim_plans`; `nb_silver_patients_scd2` for `dim_patients` (SCD Type 2, tracking `plan_id` and `state`); `nb_silver_drugs` for the shortcut-backed `dim_drugs`; `nb_silver_drug_labels` for `drug_label_enrichment`; and `nb_silver_fills` for `fact_fills_raw`, handling dedupe, date standardization, the `days_supply` fix, and prescriber-orphan quarantine.
- The gold layer, built once per run via `Invoke_GoldLayer`, `pl_gold_layer`, and `nb_gold_build_star_schema`, after `ForEach_Entity` fully completes.
- A design rule enforced throughout: cross-entity joins inside a silver notebook, such as fills joining to drugs or prescribers, always read from `Bronze_LH`, never from another entity's `Silver_LH` output. This avoids race conditions given that `ForEach_Entity` runs entities in parallel with no guaranteed ordering between them. The gold notebook is the one place where reading another entity's silver output is safe, precisely because it only runs after the whole ForEach is done.
- Eventstream for `streaming_pos_events`. Source, destination, and producer script are all verified working end to end, with 500 events confirmed landed in `Bronze_LH.dbo.streaming_pos_events`.
- `pl_fills_incremental`, a dedicated pipeline with genuine watermark-based incremental file loading, a fixed race-condition gap so the watermark now reflects true file-modified time rather than pipeline-completion time, and a working, confirmed event-driven trigger, where uploads to `Files/raw/fact_fills_raw/` fire the pipeline automatically with no manual run needed.
- The semantic model, `Pharmacy_Analytics_SM`, in Direct Lake mode, with 8 tables, 6 relationships, and 4 measures, all verified working. See `semantic-model-design.md` for full detail.
- Error handling, logging, and Notify, built on `pl_fills_incremental` as a reference pattern, deliberately not replicated to `pl_master_orchestrator`. This is a documented scope decision.
- Dataflow Gen2 (`df_drug_reference_mart`), built and verified mechanically working. The specific drug-reference blend produced no matches, documented as a data limitation rather than a tool failure.
- Dynamic Data Masking on `pharmacy_opsdb.dbo.patients`, applied and verified via `sys.masked_columns` metadata.

## Not yet built

- Power BI report pages and RLS. These are explicitly out of scope now, per the access restriction and the data-engineering-versus-BI scope reasoning above.
- A CI/CD deployment pipeline and additional data quality checks. These are still just documented and discussed, not started.
- Error handling, logging, and Notify on `pl_master_orchestrator`. This is deliberately not built. `pl_fills_incremental` stands as the reference implementation of a directly reusable pattern.

## Not pursued (explicit decision, not an oversight)

- Kafka for `streaming_pos_events`. This was prototyped, including a producer script, a docker-compose file, and a Confluent Cloud guide, then explicitly reverted back to the simpler Eventstream sample-data approach per instruction. The Kafka files remain in `/streaming/` if revisited later.
- The `azure-eventhub` SDK for the Eventstream producer. This was blocked by the package install issue above. The REST and HTTPS version with a SAS token is the one actually in use, not a temporary workaround pending a fix.
- Row-level watermarking for `fact_fills_raw`. This isn't possible with a flat-file Copy Data source. File-level, last-modified watermarking was used instead, which is the standard real-world pattern for file-based incremental loads anyway.
- The full Power BI report and RLS. See the semantic model section above. This was blocked by the access restriction, and it's arguably outside core data engineering scope regardless.
- Error handling and logging replicated across every pipeline branch. This was built once as a reference pattern on `pl_fills_incremental`, deliberately not copied to the other 6 entities in `pl_master_orchestrator`.
- A live behavioral test of Dynamic Data Masking, and governance sensitivity labels beyond the attempt made. Both hit genuine platform or tenant-configuration walls, documented in their dedicated sections above rather than repeated here.

## Next steps: packaging only, no further building planned

1. Assemble the demo video from captured clips: a pipeline run with parallel entities, a quarantined-row catch, incremental-load count growth, the file-arrival trigger firing automatically, Eventstream throughput live, the semantic model's relationship diagram, and the closing `Gold_LH` opioid-utilization query.
2. Manually assemble the GitHub repo, since Git integration remains blocked. This means gathering notebook exports (`.ipynb` and the `.py` files already delivered throughout this project), the dataset and control tables, `PROJECT_STATUS.md`, `semantic-model-design.md`, the README, and screenshots of every canvas (pipelines, semantic model, Eventstream, the Activator rule, and the DDM metadata query) as supporting evidence for what can't be exported as a literal file.
3. A final README pass to update for the dropped Power BI scope, the semantic model, the error-handling and logging layer, the DDM and governance tag outcomes, and the full 22-item bug list, once the above are in place.
4. If CI/CD is ever unblocked, pending the Git integration question with the tenant admin, it remains the one item with no current workaround. Everything else in this project has either been built, or has a documented, deliberate reason it wasn't.
