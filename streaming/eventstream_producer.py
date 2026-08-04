"""
eventstream_producer.py (REST/HTTP version — no azure-eventhub package needed)

Publishes streaming_pos_events.jsonl to a Fabric Eventstream's Custom App
source over plain HTTPS, authenticated with a SAS token generated from
the same connection string the SDK version would have used. Only needs
`requests` (already proven available) plus Python's standard library —
no extra pip install that might be blocked by an environment's package
restrictions.

No command-line arguments — edit the CONFIG values below, then run.
"""

import base64
import hashlib
import hmac
import json
import sys
import time
import urllib.parse
from pathlib import Path

import requests

# ============================================================
# CONFIG — edit these values, then just run the script
# ============================================================

# Paste the full "Connection string-primary key" from the Eventstream's
# Live view -> pos_producer node -> Details -> Event Hub -> Keys tab.
CONNECTION_STR = "PASTE_YOUR_CONNECTION_STRING_HERE"

# Path to the dataset file (use "Copy File API Path" from the Lakehouse
# Files pane on the uploaded file to get this exactly right).
SOURCE_FILE = "streaming_pos_events.jsonl"

# True  = just print what would be sent, no real connection (safe to test first)
# False = actually connect and send events for real
DRY_RUN = True

# True  = replay the file continuously, forever (stop the cell to end)
# False = send the file through exactly once, then stop
LOOP = False

# Seconds to wait between each event sent (0 = send as fast as possible)
DELAY_SECONDS = 0.5

# ============================================================
# Script logic — no need to edit below this line
# ============================================================


def parse_connection_string(conn_str: str) -> dict:
    # Defensive parsing: tolerates stray whitespace/newlines that can get
    # picked up accidentally when copying the connection string out of the
    # Fabric portal (a very easy copy-paste artifact), rather than crashing
    # on a segment that doesn't contain "=".
    parts = {}
    for p in conn_str.strip().split(";"):
        p = p.strip()
        if not p or "=" not in p:
            continue
        k, v = p.split("=", 1)
        parts[k.strip()] = v.strip()

    namespace = parts["Endpoint"].replace("sb://", "").rstrip("/")
    return {
        "namespace": namespace,
        "key_name": parts["SharedAccessKeyName"],
        "key": parts["SharedAccessKey"],
        "entity_path": parts["EntityPath"],
    }


def generate_sas_token(uri: str, key_name: str, key: str, ttl_seconds: int = 3600) -> str:
    target_uri = urllib.parse.quote_plus(uri).lower()
    expiry = int(time.time() + ttl_seconds)
    to_sign = f"{target_uri}\n{expiry}".encode("utf-8")
    signature = base64.b64encode(hmac.new(key.encode("utf-8"), to_sign, hashlib.sha256).digest())
    return (
        f"SharedAccessSignature sr={target_uri}"
        f"&sig={urllib.parse.quote(signature)}"
        f"&se={expiry}&skn={key_name}"
    )


def send_event_http(conn_info: dict, event: dict) -> requests.Response:
    uri = f"https://{conn_info['namespace']}/{conn_info['entity_path']}"
    token = generate_sas_token(uri, conn_info["key_name"], conn_info["key"])
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
    }
    url = f"{uri}/messages?timeout=60&api-version=2014-01"
    resp = requests.post(url, headers=headers, data=json.dumps(event), timeout=30)
    resp.raise_for_status()
    return resp


def load_events(source_file: str):
    events = []
    with open(source_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
    return events


def main():
    source_path = Path(SOURCE_FILE)
    if not source_path.exists():
        print(f"Source file not found: {source_path}", file=sys.stderr)
        sys.exit(1)

    events = load_events(str(source_path))
    if not events:
        print("No events found in source file.", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(events)} events from {source_path}")

    if DRY_RUN:
        print("-- DRY_RUN = True: no Eventstream connection made --")
        for e in events[:3]:
            print(f"  {json.dumps(e)[:120]}...")
        print(f"  ... and {max(0, len(events) - 3)} more")
        print("\nSet DRY_RUN = False at the top of this file once you're ready to send for real.")
        return

    if CONNECTION_STR == "PASTE_YOUR_CONNECTION_STRING_HERE":
        print("Error: edit CONNECTION_STR at the top of this file first.", file=sys.stderr)
        sys.exit(1)

    conn_info = parse_connection_string(CONNECTION_STR)
    sent = 0
    try:
        while True:
            for event in events:
                send_event_http(conn_info, event)
                sent += 1
                if DELAY_SECONDS:
                    time.sleep(DELAY_SECONDS)
            print(f"Sent {sent} events so far")
            if not LOOP:
                break
    except KeyboardInterrupt:
        print(f"\nStopped. Sent {sent} events total.")


if __name__ == "__main__":
    main()
else:
    main()
