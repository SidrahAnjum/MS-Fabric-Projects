# nb_setup_run_log
#
# One-time setup: creates the pipeline_run_log table in Bronze_LH.
# Run once; safe to re-run (IF NOT EXISTS guards against duplicating
# an existing table with real data in it).

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_run_log (
        run_id          STRING,
        pipeline_name   STRING,
        entity_name     STRING,
        status          STRING,
        rows_processed  INT,
        error_message   STRING,
        run_timestamp   TIMESTAMP
    )
    USING DELTA
""")

print("pipeline_run_log table ready.")
spark.sql("DESCRIBE pipeline_run_log").show()
