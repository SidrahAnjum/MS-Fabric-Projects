# nb_update_watermark
#
# Advances the stored watermark for fact_fills_raw to the actual latest
# file-modified time seen among files in the source folder — NOT "now" —
# closing a race-condition gap where a file uploaded between Copy_File_Source's
# read and this notebook's run could otherwise be silently skipped forever.
# The pipeline computes this value (via GetMetadata_ListFiles + ForEach_File +
# a running-max variable) and passes it in as new_watermark_value.
#
# Lakehouse setup: Bronze_LH attached as default.

# Parameters cell
entity_name = "fact_fills_raw"
new_watermark_value = "PASSED_IN_FROM_PIPELINE"  # overridden by the pipeline's Base parameters

spark.sql(f"""
    UPDATE pipeline_watermark
    SET last_watermark_utc = '{new_watermark_value}'
    WHERE entity_name = '{entity_name}'
""")

new_value = spark.sql(
    f"SELECT last_watermark_utc FROM pipeline_watermark WHERE entity_name = '{entity_name}'"
).collect()[0]["last_watermark_utc"]

print(f"Watermark for {entity_name} advanced to: {new_value}")

