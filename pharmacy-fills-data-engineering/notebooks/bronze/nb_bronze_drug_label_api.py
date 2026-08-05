# nb_bronze_drug_label_api
#
# Bronze-layer ingestion for drug_label_enrichment. Pulls supplemental
# drug label data from the public openFDA API, handling pagination in
# Python rather than chaining Web/ForEach/Append Variable activities on
# the pipeline canvas — more robust, easier to debug.
#
# Lakehouse setup: Bronze_LH attached as default.

# Parameters cell (mark as "Parameters" in Fabric so the pipeline can
# inject values via Base parameters)
api_base_url = "https://api.fda.gov/drug/label.json"
page_size = 100
max_records = 5000  # demo cap — openFDA allows much higher, kept small for a portfolio run
sink_table = "drug_label_enrichment"
entity_name = "drug_label_enrichment"

import json
import requests
from pyspark.sql import functions as F


def fetch_page(skip: int, limit: int) -> dict:
    resp = requests.get(api_base_url, params={"limit": limit, "skip": skip}, timeout=30)
    resp.raise_for_status()
    return resp.json()


first_page = fetch_page(skip=0, limit=page_size)
total_available = first_page.get("meta", {}).get("results", {}).get("total", 0)
all_results = list(first_page.get("results", []))
print(f"Total records available from API: {total_available}")

target = min(total_available, max_records)
skip = page_size
while skip < target:
    page = fetch_page(skip=skip, limit=page_size)
    results = page.get("results", [])
    if not results:
        break
    all_results.extend(results)
    skip += page_size

print(f"Fetched {len(all_results)} records across {skip // page_size + 1} pages")

# openFDA responses are deeply nested — flatten to a landing-friendly shape
flattened = []
for r in all_results:
    ofda = r.get("openfda", {})
    flattened.append({
        "ndc": (ofda.get("product_ndc") or [None])[0],
        "brand_name": (ofda.get("brand_name") or [None])[0],
        "generic_name": (ofda.get("generic_name") or [None])[0],
        "manufacturer_name": (ofda.get("manufacturer_name") or [None])[0],
        "route": (ofda.get("route") or [None])[0],
        "warnings": (" ".join(r.get("warnings", []))[:2000] if r.get("warnings") else None),
        "_raw_json": json.dumps(r),
    })

df = spark.createDataFrame(flattened)
df = df.withColumn("_ingested_at", F.current_timestamp())

df.write.format("delta").mode("overwrite").saveAsTable(sink_table)
print(f"Wrote {df.count()} rows to {sink_table}")
