"""Fetch enum values for all custom types from the PostgREST OpenAPI spec."""
import json
import os
import urllib.request
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://cpfdvfvdqxajynlqunzl.supabase.co")
SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

if not SERVICE_ROLE_KEY:
    raise SystemExit("Set SUPABASE_SERVICE_ROLE_KEY and retry.")

req = urllib.request.Request(
    SUPABASE_URL.rstrip("/") + "/rest/v1/",
    headers={
        "apikey": SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
    },
)

with urllib.request.urlopen(req, timeout=15) as resp:
    spec = json.loads(resp.read())

# Collect enum values from all property definitions
enum_map = defaultdict(set)

for table_name, defn in spec.get("definitions", {}).items():
    for col, meta in defn.get("properties", {}).items():
        fmt = meta.get("format", "")
        enum_vals = meta.get("enum", [])
        if fmt.startswith("public.") and enum_vals:
            enum_map[fmt].update(enum_vals)

if not enum_map:
    print("No enum values found in OpenAPI spec properties.")
    print("Trying definitions-level enums...")
    for name, defn in spec.get("definitions", {}).items():
        if "enum" in defn:
            print(f"\n{name}: {defn['enum']}")
else:
    print("Enum values by type:\n")
    for enum_type, values in sorted(enum_map.items()):
        print(f"{enum_type}:")
        for v in sorted(values):
            print(f"  '{v}'")
        print()
