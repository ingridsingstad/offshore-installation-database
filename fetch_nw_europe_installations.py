"""
Henter offshore installasjoner for UK, Danmark og Nederland fra offisielle kilder
og lagrer til nw_europe_installations.json.

Kjør én gang lokalt:
    python3 fetch_nw_europe_installations.py

Kilder:
  - UK:           NSTA ArcGIS REST API (data.nstauthority.co.uk)
  - Danmark:      ENS via GEUS WFS (data.geus.dk)
  - Nederland:    NLOG via GDN GeoServer WFS (gdngeoservices.nl)
"""

import json
import urllib.request
import urllib.parse
import urllib.error
import xml.etree.ElementTree as ET
import datetime
import sys
import time

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; research-script/1.0)"}

# ── Status-normalisering ───────────────────────────────────────────────────────
UK_STATUS = {
    "active": "operating",
    "in service": "operating",
    "producing": "operating",
    "decommissioned": "abandoned",
    "removed": "abandoned",
    "decommissioning": "decommissioning",
    "under construction": "in-development",
    "proposed": "in-development",
    "approved": "in-development",
    "installation": "in-development",
    "care and maintenance": "mothballed",
    "shut in": "mothballed",
    "idle": "mothballed",
}

DK_STATUS = {
    "in production": "operating",
    "producing": "operating",
    "active": "operating",
    "in service": "operating",
    "production": "operating",
    "decommissioned": "abandoned",
    "removed": "abandoned",
    "abandoned": "abandoned",
    "shut down": "mothballed",
    "inactive": "mothballed",
    "under construction": "in-development",
    "planned": "in-development",
}

NL_STATUS = {
    "in productie": "operating",
    "in gebruik": "operating",
    "active": "operating",
    "actief": "operating",
    "in use": "operating",
    "in service": "operating",
    "production": "operating",
    "operational": "operating",
    "ontmanteld": "abandoned",
    "verwijderd": "abandoned",
    "decommissioned": "abandoned",
    "removed": "abandoned",
    "abandoned": "abandoned",
    "plugged and abandoned": "abandoned",
    "in aanleg": "in-development",
    "gepland": "in-development",
    "under construction": "in-development",
    "planned": "in-development",
    "buiten gebruik": "mothballed",
    "inactief": "mothballed",
    "inactive": "mothballed",
    "shut in": "mothballed",
    "temporarily abandoned": "mothballed",
}


def normalize_status(raw, mapping):
    if not raw:
        return "unknown"
    return mapping.get(raw.lower().strip(), "unknown")


# ── HTTP helpers ───────────────────────────────────────────────────────────────
def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def http_get_json(url, timeout=30):
    data = http_get(url, timeout)
    return json.loads(data)


# ══════════════════════════════════════════════════════════════════════════════
# UK — NSTA ArcGIS REST API
# ══════════════════════════════════════════════════════════════════════════════

def fetch_uk_nsta():
    print("\n── UK: NSTA ArcGIS REST ──────────────────────────────────────")

    layers = [
        (0, "Surface (platforms/FPSOs)"),
        (1, "Subsea (wellheads/manifolds)"),
    ]
    BASE = (
        "https://data.nstauthority.co.uk/arcgis/rest/services/"
        "Public_WGS84/UKCS_Offshore_Infrastructure_WGS84/FeatureServer"
    )

    # Test connection first
    try:
        test_url = f"{BASE}/0?f=json"
        info = http_get_json(test_url, timeout=15)
        if "error" in info:
            raise RuntimeError(str(info["error"]))
        print(f"  Tilkoblet: {info.get('name','NSTA Layer 0')}")
    except Exception as e:
        print(f"  [FEIL] Kan ikke nå NSTA: {e}")
        print("  Tips: Koble fra VPN og prøv igjen.")
        return []

    all_records = []

    for layer_id, layer_label in layers:
        query_url = f"{BASE}/{layer_id}/query"
        print(f"  Henter lag {layer_id}: {layer_label}...")

        # Get layer info for field names
        try:
            layer_info = http_get_json(f"{BASE}/{layer_id}?f=json")
            fields = [f["name"] for f in layer_info.get("fields", [])]
        except Exception:
            fields = []

        def pick(*candidates):
            for c in candidates:
                if c in fields:
                    return c
            return None

        name_f   = pick("NAME", "Name", "INFRA_NAME", "FACILITY_NAME", "InstallationName")
        type_f   = pick("INF_TYPE", "Type", "INFRASTRUCTURE_TYPE", "SubType")
        status_f = pick("Status", "STATUS", "ACTIVE_STATUS")
        op_f     = pick("REP_GROUP", "Operator", "OPERATOR", "COMPANY")

        print(f"  Felt: navn={name_f}, type={type_f}, status={status_f}, operatør={op_f}")

        # Fetch all ObjectIDs first
        try:
            ids_params = urllib.parse.urlencode({
                "where": "1=1",
                "returnIdsOnly": "true",
                "f": "json"
            })
            ids_data = http_get_json(f"{query_url}?{ids_params}")
            all_ids = ids_data.get("objectIds") or []
            print(f"  {len(all_ids)} objekter funnet i lag {layer_id}")
        except Exception as e:
            print(f"  [FEIL] Henting av IDs feilet: {e}")
            continue

        # Fetch in batches of 200
        out_fields = ",".join(f for f in [name_f, type_f, status_f, op_f] if f) or "*"
        layer_records = []
        BATCH = 200

        for i in range(0, len(all_ids), BATCH):
            batch = all_ids[i:i + BATCH]
            params = urllib.parse.urlencode({
                "objectIds": ",".join(str(x) for x in batch),
                "outFields": out_fields,
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "json"
            })
            try:
                data = http_get_json(f"{query_url}?{params}")
                if "error" in data:
                    print(f"  [FEIL] Batch {i}: {data['error']}")
                    continue
                features = data.get("features", [])
                for feat in features:
                    a = feat.get("attributes", {})
                    g = feat.get("geometry")
                    lat = g["y"] if g else None
                    lon = g["x"] if g else None

                    def pv(field):
                        if not field:
                            return None
                        v = a.get(field)
                        return str(v).strip() if v not in (None, "", "None", "Null") else None

                    name = pv(name_f) or "Unknown"
                    layer_records.append({
                        "installation_name": name,
                        "country": "United Kingdom",
                        "region": "UK Continental Shelf",
                        "fuel_type": "oil and gas",
                        "production_type": None,
                        "status": normalize_status(pv(status_f), UK_STATUS),
                        "operator": pv(op_f) or "Unknown",
                        "onshore_offshore": "offshore",
                        "latitude": lat,
                        "longitude": lon,
                        "year_production_start": None,
                        "year_discovered": None,
                        "basin": None,
                        "block": None,
                        "owners": None,
                        "wiki_url": None,
                        "source_authority": "NSTA",
                        "data_type": "installation",
                        "installation_type": pv(type_f) or "Unknown",
                        "parent_field": None,
                        "water_depth": None,
                        "fact_page_url": None,
                        "id": name,
                    })
                print(f"  Hentet {len(layer_records)} fra lag {layer_id}...", end="\r")
            except Exception as e:
                print(f"\n  [FEIL] Batch {i}: {e}")
                time.sleep(1)

        print(f"  -> {len(layer_records)} UK-installasjoner fra lag {layer_id}")
        all_records.extend(layer_records)

    print(f"  UK totalt: {len(all_records)} installasjoner")
    return all_records


# ══════════════════════════════════════════════════════════════════════════════
# Danmark — ENS via GEUS WFS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_denmark_ens():
    print("\n── Danmark: ENS via GEUS WFS ─────────────────────────────────")

    WFS_URL = "https://data.geus.dk/geusmap/ows/4326.jsp"
    TYPENAME = "ens_platform"

    # Step 1: Hent schema
    field_names = []
    try:
        params = urllib.parse.urlencode({
            "service": "WFS", "version": "2.0.0",
            "request": "DescribeFeatureType", "typeName": TYPENAME
        })
        xml_data = http_get(f"{WFS_URL}?{params}", timeout=30)
        root = ET.fromstring(xml_data)
        for el in root.iter():
            tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if tag in ("element", "Element"):
                nm = el.get("name") or el.get("Name")
                if nm:
                    field_names.append(nm)
        print(f"  Feltnavn funnet: {field_names[:10]}")
    except Exception as e:
        print(f"  [ADVARSEL] Schema-henting feilet: {e}")

    # Step 2: Hent features — prøv flere output-formater
    features = []
    output_formats = [
        "application/json",
        "json",
        "application/json; subtype=geojson/1.0",
        "GeoJSON",
    ]
    for fmt in output_formats:
        params = urllib.parse.urlencode({
            "service": "WFS", "version": "1.1.0", "request": "GetFeature",
            "typeName": TYPENAME, "outputFormat": fmt, "srsName": "EPSG:4326",
            "maxFeatures": "2000"
        })
        try:
            raw = http_get(f"{WFS_URL}?{params}", timeout=60)
            data = json.loads(raw)
            features = data.get("features", [])
            if features:
                print(f"  {len(features)} features hentet (format: {fmt})")
                break
        except Exception as e:
            print(f"  [prøver neste format etter feil med '{fmt}': {e}]")
            continue

    if not features:
        print("  [FEIL] Ingen data fra ENS Danmark med noen output-format.")
        return []

    if not features:
        print("  [FEIL] Ingen data fra ENS.")
        return []

    # Detect field names from first feature
    sample = features[0].get("properties") or {}
    if not field_names:
        field_names = list(sample.keys())
        print(f"  Felt fra data: {field_names[:10]}")

    fl = {f.lower(): f for f in field_names}

    def pick(*candidates):
        for c in candidates:
            if c.lower() in fl:
                return fl[c.lower()]
        return None

    name_f   = pick("platform_name", "name", "navn", "label", "installationname", "platformname")
    type_f   = pick("platform_type_name", "platform_type", "category_name", "type", "kind", "category", "installation_type")
    status_f = pick("status", "driftstatus", "status_code", "phase", "in_use", "function_name")
    op_f     = pick("operator_name", "company_name", "company", "operator", "licensee", "owner", "licenseholder")
    field_f  = pick("field_name", "field", "felt", "production_unit", "fieldname")

    print(f"  Feltmapping: navn={name_f}, type={type_f}, status={status_f}, op={op_f}, felt={field_f}")

    records = []
    for feat in features:
        props = feat.get("properties") or {}
        geom  = feat.get("geometry") or {}
        coords = geom.get("coordinates", [None, None])
        lat = float(coords[1]) if len(coords) > 1 and coords[1] is not None else None
        lon = float(coords[0]) if len(coords) > 0 and coords[0] is not None else None

        def pv(field):
            if not field:
                return None
            v = props.get(field)
            return str(v).strip() if v not in (None, "", "None", "nan", "NULL") else None

        name = pv(name_f)
        if not name:
            # Try any string field
            for k, v in props.items():
                if v and str(v).strip() not in ("", "None", "nan") and len(str(v)) > 2 and any(c.isalpha() for c in str(v)):
                    name = str(v).strip()
                    break
        if not name:
            name = "Unknown"

        records.append({
            "installation_name": name,
            "country": "Denmark",
            "region": "Danish Continental Shelf",
            "fuel_type": "oil and gas",
            "production_type": None,
            "status": normalize_status(pv(status_f), DK_STATUS),
            "operator": pv(op_f) or "Unknown",
            "onshore_offshore": "offshore",
            "latitude": lat,
            "longitude": lon,
            "year_production_start": None,
            "year_discovered": None,
            "basin": pv(field_f),
            "block": None,
            "owners": None,
            "wiki_url": None,
            "source_authority": "ENS Denmark / GEUS",
            "data_type": "installation",
            "installation_type": pv(type_f) or "Unknown",
            "parent_field": pv(field_f),
            "water_depth": None,
            "fact_page_url": None,
            "id": name,
        })

    print(f"  -> {len(records)} installasjoner fra ENS (Danmark)")
    return records


# ══════════════════════════════════════════════════════════════════════════════
# Nederland — NLOG via GDN GeoServer WFS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_netherlands_nlog():
    print("\n── Nederland: NLOG via GDN GeoServer WFS ─────────────────────")

    WFS_URL  = "https://www.gdngeoservices.nl/geoserver/nlog/ows"
    TYPENAME = "nlog:GDW_NG_FACILITY_UTM"

    # Step 1: Hent schema
    field_names = []
    try:
        params = urllib.parse.urlencode({
            "service": "WFS", "version": "2.0.0",
            "request": "DescribeFeatureType", "typeName": TYPENAME
        })
        xml_data = http_get(f"{WFS_URL}?{params}", timeout=30)
        root = ET.fromstring(xml_data)
        for el in root.iter():
            tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if tag in ("element", "Element"):
                nm = el.get("name") or el.get("Name")
                if nm:
                    field_names.append(nm)
        print(f"  Feltnavn funnet: {field_names[:15]}")
    except Exception as e:
        print(f"  [ADVARSEL] Schema-henting feilet: {e}")

    # Step 2: Hent features (request WGS84 direkte)
    params = urllib.parse.urlencode({
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeName": TYPENAME, "outputFormat": "application/json", "srsName": "EPSG:4326"
    })
    try:
        data = http_get_json(f"{WFS_URL}?{params}", timeout=90)
        features = data.get("features", [])
        print(f"  {len(features)} features hentet fra NLOG (Nederland)")
    except Exception as e:
        print(f"  [FEIL] NLOG WFS: {e}")
        return []

    if not features:
        print("  [FEIL] Ingen data fra NLOG.")
        return []

    sample = features[0].get("properties") or {}
    if not field_names:
        field_names = list(sample.keys())

    print(f"  Eksempel-felt: {list(sample.keys())[:12]}")

    fl = {f.lower(): f for f in field_names}

    def pick(*candidates):
        for c in candidates:
            if c.lower() in fl:
                return fl[c.lower()]
        return None

    name_f   = pick("FACILITY_NAME", "NAAM", "naam", "NAME", "name", "label", "OMSCHRIJVING")
    type_f   = pick("FACILITY_TYPE_DESCRIPTION", "FACILITY_TYPE_CODE", "TYPE", "type", "SOORT", "KIND")
    status_f = pick("STATUS_DESCRIPTION", "STATUS_CODE", "STATUS", "status", "DRIFTSTATUS", "IN_GEBRUIK")
    op_f     = pick("OPERATOR", "MAATSCHAPPIJ", "maatschappij", "operator", "COMPANY", "NAAM_MAA")
    field_f  = pick("VELD", "veld", "FIELD_NAME", "VELDNAAM", "FIELD", "PRODUCTION_UNIT")

    print(f"  Feltmapping: naam={name_f}, type={type_f}, status={status_f}, op={op_f}, veld={field_f}")

    records = []
    for feat in features:
        props = feat.get("properties") or {}
        geom  = feat.get("geometry") or {}
        coords = geom.get("coordinates", [None, None])

        # GeoServer with srsName=EPSG:4326 should return [lon, lat]
        try:
            lon = float(coords[0]) if coords[0] is not None else None
            lat = float(coords[1]) if len(coords) > 1 and coords[1] is not None else None
        except (TypeError, ValueError):
            lat = lon = None

        # Sanity check: Netherlands is ~51-54°N, 3-7°E
        if lat and lon:
            if not (48 < lat < 58 and -5 < lon < 10):
                # Might be UTM — skip coord (can't convert without pyproj)
                lat = lon = None

        def pv(field):
            if not field:
                return None
            v = props.get(field)
            return str(v).strip() if v not in (None, "", "None", "nan", "NULL", "null") else None

        name = pv(name_f)
        if not name:
            for k, v in props.items():
                if v and str(v).strip() not in ("", "None", "nan") and len(str(v)) > 2 and any(c.isalpha() for c in str(v)):
                    name = str(v).strip()
                    break
        if not name:
            name = "Unknown"

        records.append({
            "installation_name": name,
            "country": "Netherlands",
            "region": "Dutch Continental Shelf",
            "fuel_type": "oil and gas",
            "production_type": None,
            "status": normalize_status(pv(status_f), NL_STATUS),
            "operator": pv(op_f) or "Unknown",
            "onshore_offshore": "offshore",
            "latitude": lat,
            "longitude": lon,
            "year_production_start": None,
            "year_discovered": None,
            "basin": pv(field_f),
            "block": None,
            "owners": None,
            "wiki_url": None,
            "source_authority": "NLOG Netherlands",
            "data_type": "installation",
            "installation_type": pv(type_f) or "Unknown",
            "parent_field": pv(field_f),
            "water_depth": None,
            "fact_page_url": None,
            "id": name,
        })

    print(f"  -> {len(records)} installasjoner fra NLOG (Nederland)")
    return records


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 62)
    print("  NW EUROPE OFFSHORE INSTALLATIONS — DATAINNHENTING")
    print(f"  {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 62)

    all_records = []

    uk_records = fetch_uk_nsta()
    all_records.extend(uk_records)

    dk_records = fetch_denmark_ens()
    all_records.extend(dk_records)

    nl_records = fetch_netherlands_nlog()
    all_records.extend(nl_records)

    print(f"\n{'='*62}")
    print(f"  TOTALT: {len(all_records)} installasjoner")
    print(f"  UK:          {len(uk_records)}")
    print(f"  Danmark:     {len(dk_records)}")
    print(f"  Nederland:   {len(nl_records)}")
    print(f"{'='*62}")

    by_status = {}
    for r in all_records:
        s = r["status"]
        by_status[s] = by_status.get(s, 0) + 1

    print("\nStatus-fordeling:")
    for s, n in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f"  {s:30s} {n}")

    with_coords = sum(1 for r in all_records if r["latitude"] and r["longitude"])
    print(f"\nMed koordinater: {with_coords} / {len(all_records)}")

    output = {
        "source": "NSTA (UK) + ENS/GEUS (Denmark) + NLOG (Netherlands)",
        "license": "Open Government Data",
        "fetched": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_records": len(all_records),
        "breakdown": {
            "UK": len(uk_records),
            "Denmark": len(dk_records),
            "Netherlands": len(nl_records),
        },
        "installations": all_records,
    }

    out_path = "nw_europe_installations.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)

    print(f"\nLagret til {out_path}")


if __name__ == "__main__":
    main()
