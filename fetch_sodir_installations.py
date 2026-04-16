"""
Henter alle NCS-installasjoner fra SODIR sitt ArcGIS REST API og lagrer som
sodir_installations.json.

Kjør én gang lokalt:
    python3 fetch_sodir_installations.py

Filen sodir_installations.json lagres i samme mappe og commites til GitHub.
"""

import json
import urllib.request
import urllib.parse
import sys
import datetime

LAYER_URL = (
    "https://factmaps.sodir.no/api/rest/services/Factmaps/"
    "FactMapsWGS84/MapServer/307"
)
QUERY_URL = LAYER_URL + "/query"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; research-script/1.0)"}

STATUS_MAP = {
    "IN SERVICE": "operating",
    "REMOVED": "abandoned",
    "PARTLY REMOVED": "abandoned",
    "ABANDONED IN PLACE": "abandoned",
    "DECOMMISSIONED": "abandoned",
    "DISPOSAL COMPLETED": "abandoned",
    "INSTALLATION": "in-development",
    "UNDER INSTALLATION": "in-development",
    "FUTURE": "in-development",
    "PLANNED": "in-development",
    "SHUT DOWN": "mothballed",
    "LAID UP": "mothballed",
    "INACTIVE": "mothballed",
}


def get(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def get_layer_info():
    """Henter lag-metadata og alle tilgjengelige feltnavn."""
    print("Henter lag-metadata fra SODIR...")
    data = get(f"{LAYER_URL}?f=json")
    if "error" in data:
        print(f"  Feil: {data['error']}")
        sys.exit(1)
    fields = [f["name"] for f in data.get("fields", [])]
    print(f"  Lag: {data.get('name','?')} — {len(fields)} felt tilgjengelig")
    return fields


def get_all_object_ids():
    """Henter alle ObjectID-er (raskeste pagineringsmetode)."""
    params = urllib.parse.urlencode({
        "where": "1=1",
        "returnIdsOnly": "true",
        "f": "json"
    })
    data = get(f"{QUERY_URL}?{params}")
    if "error" in data:
        print(f"  Feil ved henting av IDs: {data['error']}")
        sys.exit(1)
    ids = data.get("objectIds") or []
    print(f"  Totalt {len(ids)} installasjoner i databasen.")
    return ids


def fetch_batch(object_ids, out_fields):
    """Henter en batch med features etter ObjectID."""
    ids_str = ",".join(str(i) for i in object_ids)
    params = urllib.parse.urlencode({
        "objectIds": ids_str,
        "outFields": out_fields,
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json"
    })
    data = get(f"{QUERY_URL}?{params}")
    if "error" in data:
        raise RuntimeError(f"Batch-feil: {data['error']}")
    return data.get("features", [])


def normalize_phase(phase):
    return STATUS_MAP.get((phase or "").upper().strip(), "unknown")


def pick_field(attrs, *candidates):
    """Velger første felt som finnes i attributtdictionarien."""
    for c in candidates:
        if c in attrs and attrs[c] not in (None, "", "None", "nan"):
            return attrs[c]
    return None


def map_feature(f, available_fields):
    a = f.get("attributes", {})
    g = f.get("geometry")
    lat = g["y"] if g else None
    lon = g["x"] if g else None

    name = pick_field(a, "fclName", "name", "NAVN")
    kind = pick_field(a, "fclKind", "kind", "type", "TYPE")
    phase = pick_field(a, "fclPhase", "phase", "status", "STATUS")
    operator = pick_field(a, "fclCurrentOperatorName", "operator", "OPERATOR")
    field_name = pick_field(a, "fclNsdFldsName", "fclBelongsToName", "fieldName", "FIELD_NAME")
    startup = pick_field(a, "fclStartupDate", "startupDate", "START_DATE")
    water_depth = pick_field(a, "fclWaterDepth", "waterDepth", "WATER_DEPTH")
    fact_page = pick_field(a, "fclFactPageUrl", "factPageUrl")

    year = None
    if startup:
        s = str(startup).strip()
        year_str = s[:4] if len(s) >= 4 else None
        if year_str and year_str.isdigit():
            year = int(year_str)

    return {
        "installation_name": name or "Unknown",
        "country": "Norway",
        "region": "Norwegian Continental Shelf",
        "fuel_type": "oil and gas",
        "production_type": None,
        "status": normalize_phase(phase),
        "operator": operator or "Unknown",
        "onshore_offshore": "offshore",
        "latitude": lat,
        "longitude": lon,
        "year_production_start": year,
        "year_discovered": None,
        "basin": field_name,
        "block": None,
        "owners": None,
        "wiki_url": None,
        "source_authority": "SODIR",
        "data_type": "installation",
        "installation_type": kind or "Unknown",
        "parent_field": field_name,
        "water_depth": water_depth,
        "fact_page_url": fact_page,
        "id": name,
    }


def main():
    # 1. Hent lag-info og feltnavn
    available_fields = get_layer_info()

    # Bygg outFields-streng av felt vi vil ha (bare de som faktisk finnes)
    wanted = [
        "fclName", "fclKind", "fclPhase", "fclCurrentOperatorName",
        "fclNsdFldsName", "fclBelongsToName", "fclStartupDate",
        "fclWaterDepth", "fclFactPageUrl"
    ]
    out_fields = ",".join(f for f in wanted if f in available_fields)
    if not out_fields:
        # Fallback: hent alle felt
        out_fields = "*"
        print("  Ingen kjente felt funnet — bruker outFields=*")
    else:
        print(f"  Bruker felt: {out_fields}")

    # 2. Hent alle ObjectID-er
    all_ids = get_all_object_ids()
    if not all_ids:
        print("Ingen installasjoner funnet.")
        sys.exit(0)

    # 3. Hent features i batches på 200
    BATCH_SIZE = 200
    all_features = []
    for i in range(0, len(all_ids), BATCH_SIZE):
        batch = all_ids[i:i + BATCH_SIZE]
        features = fetch_batch(batch, out_fields)
        all_features.extend(features)
        print(f"  Hentet {len(all_features)} / {len(all_ids)} installasjoner...", end="\r")

    print(f"\nFerdig! {len(all_features)} installasjoner hentet.")

    # 4. Konverter til vår schema
    installations = [map_feature(f, available_fields) for f in all_features]

    # 5. Lagre til JSON
    output = {
        "source": "SODIR — Sokkeldirektoratet (Norwegian Offshore Directorate)",
        "license": "Open Government Data — sodir.no",
        "fetched": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_records": len(installations),
        "installations": installations,
    }

    out_path = "sodir_installations.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)

    print(f"Lagret til {out_path}")

    # 6. Statistikk
    by_status = {}
    for inst in installations:
        s = inst["status"]
        by_status[s] = by_status.get(s, 0) + 1

    print("\nStatus-fordeling:")
    for s, n in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f"  {s:30s} {n}")

    with_coords = sum(1 for i in installations if i["latitude"] and i["longitude"])
    print(f"\nMed koordinater: {with_coords} / {len(installations)}")


if __name__ == "__main__":
    main()
