# nb_log_run
#
# Writes one row to pipeline_run_log, regardless of whether the upstream
# activity succeeded or failed. Called with a "Completed" dependency
# condition (not "Succeeded") from whatever activity it's logging, so it
# fires either way — that's what lets one logging step handle both
# outcomes instead of needing separate success/failure paths.
#
# All values are passed in as Base parameters from the pipeline — this
# notebook doesn't compute anything itself, just records what the
# pipeline already knows.

# Parameters cell
run_id = "unknown"
pipeline_name = "unknown"
entity_name = "unknown"
status = "unknown"
rows_processed = 0
error_message = ""

spark.sql(f"""
    INSERT INTO pipeline_run_log
    VALUES (
        '{run_id}',
        '{pipeline_name}',
        '{entity_name}',
        '{status}',
        {rows_processed},
        '{error_message.replace("'", "''")}',
        current_timestamp()
    )
""")

print(f"Logged: {pipeline_name} / {entity_name} — {status} ({rows_processed} rows)")
