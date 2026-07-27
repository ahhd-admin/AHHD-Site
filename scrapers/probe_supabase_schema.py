"""
Quick schema probe — fetches the PostgREST OpenAPI spec and prints every
public table with its columns and types.

Usage:
    Add SUPABASE_SERVICE_ROLE_KEY to AHHD-Site/.env, then run:
        python AHHD-Site/scrapers/probe_supabase_schema.py
"""

import json
import os
import urllib.request

from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://cpfdvfvdqxajynlqunzl.supabase.co")
SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

if not SERVICE_ROLE_KEY:
    raise SystemExit("Set SUPABASE_SERVICE_ROLE_KEY in your .env file and retry.")

url = SUPABASE_URL.rstrip("/") + "/rest/v1/"
req = urllib.request.Request(
    url,
    headers={
        "apikey": SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
    },
)

with urllib.request.urlopen(req, timeout=15) as resp:
    spec = json.loads(resp.read())

definitions = spec.get("definitions", {})
if not definitions:
    print("No table definitions found in OpenAPI spec.")
    print(json.dumps(spec, indent=2)[:2000])
else:
    for table_name, defn in sorted(definitions.items()):
        props = defn.get("properties", {})
        required = set(defn.get("required", []))
        print(f"\n{'='*60}")
        print(f"TABLE: {table_name}")
        print(f"{'='*60}")
        for col, meta in props.items():
            col_type = meta.get("type") or meta.get("format") or "unknown"
            fmt = meta.get("format", "")
            nullable = "" if col in required else " (nullable)"
            desc = meta.get("description", "")
            type_str = f"{col_type}({fmt})" if fmt and fmt != col_type else col_type
            print(f"  {col:<35} {type_str}{nullable}")
            if desc:
                print(f"    -> {desc}")
