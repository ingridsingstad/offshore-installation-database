"""
Global Offshore Installation Data Extractor — v4 (Northern Europe + USA)
=========================================================================
Kilder:
  Norway  — SODIR ArcGIS REST API (lag 307, alle fasiliteter)
  UK      — NSTA ArcGIS FeatureServer (Surface Points, UKCS)
  Denmark — Danish Energy Agency (ENS) WFS, lag ens_platform
  NL      — NLOG WFS (gdngeoservices.nl), lag nlog:platform
  USA     — BSEE platstrufixed.zip + platlocfixed.zip (Gulf of Mexico)

Krav:
  pip3 install requests pandas

Kjøring:
  python3 get_offshore_data.py
"""

import requests
import pandas as pd
import zipfile
import io
import json
import xml.etree.ElementTree as ET
import os
from datetime import datetime, timezone

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

global_inventory = []
missing_data_log = []


# ══════════════════════════════════════════════════════════════════════════════
# HJELPEFUNKSJONER
# ══════════════════════════════════════════════════════════════════════════════

def arcgis_query_all(base_url, label, country, region, name_fields,
                     type_field=None, operator_field=None, status_field=None,
                     source_authority=None):
    """Hent alle features fra en ArcGIS REST query-URL med automatisk paginering."""
    print(f"Kobler til {label}...")
    all_features = []
    offset = 0
    page_size = 1000

    while True:
        params = {
            "f": "json",
            "where": "1=1",
            "outFields": "*",
            "resultRecordCount": page_size,
            "resultOffset": offset,
            "returnGeometry": "true",
        }
        try:
            r = requests.get(base_url, params=params, headers=HEADERS, timeout=60)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  [FEIL] {label}: {e}")
            break

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        print(f"  {label}: {len(all_features)} rader hentet...")

        if len(features) < page_size:
            break
        offset += page_size

    added = 0
    for feature in all_features:
        attrs = feature.get("attributes", {}) or {}
        geom  = feature.get("geometry", {}) or {}

        # Finn navn (prøv feltene i prioritert rekkefølge)
        name = None
        for f in name_fields:
            v = attrs.get(f)
            if v and str(v).strip() not in ("", "None", "nan", "NULL", "<Null>"):
                name = str(v).strip()
                break

        inst_type = str(attrs.get(type_field, "Unknown")).strip() if type_field else "Unknown"
        operator  = str(attrs.get(operator_field, "Unknown")).strip() if operator_field else "Unknown"
        status_raw = str(attrs.get(status_field, "")).strip() if status_field else ""

        entry = {
            "installation_name": name,
            "country": country,
            "region": region,
            "installation_type": inst_type,
            "operator": operator,
            "status": status_raw if status_raw else "Unknown",
            "year_installed": None,
            "latitude": geom.get("y"),
            "longitude": geom.get("x"),
            "source_url": base_url,
            "source_authority": source_authority or label,
            "confidence_level": "High",
        }
        global_inventory.append(entry)
        added += 1

    print(f"  -> {added} installasjoner fra {label} lagt til.")


def wfs_query(wfs_url, typename, label, country, region,
              name_fields, type_field=None, source_authority=None):
    """Hent features fra en OGC WFS-tjeneste (forsøker GeoJSON, fallback GML)."""
    print(f"Kobler til {label} via WFS...")

    # Forsøk 1: GeoJSON
    params_json = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": typename,
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
    }
    features = []
    used_format = None

    try:
        r = requests.get(wfs_url, params=params_json, headers=HEADERS, timeout=90)
        if r.status_code == 200 and "features" in r.text[:500]:
            data = r.json()
            features = data.get("features", [])
            used_format = "GeoJSON"
    except Exception:
        pass

    # Forsøk 2: GML2 (fallback)
    if not features:
        params_gml = {
            "SERVICE": "WFS",
            "VERSION": "1.0.0",
            "REQUEST": "GetFeature",
            "TYPENAME": typename,
            "outputformat": "gml2",
        }
        try:
            r = requests.get(wfs_url, params=params_gml, headers=HEADERS, timeout=90)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            ns = {k: v for k, v in [n.split("=") if "=" in n else ("", n)
                                     for n in root.tag.replace("{", " {").split()]}
            # Parse GML features
            for member in root.iter():
                tag = member.tag.split("}")[-1] if "}" in member.tag else member.tag
                if "featureMember" in tag or "member" == tag:
                    for child in member:
                        props = {}
                        geom_coords = None
                        for prop in child:
                            pname = prop.tag.split("}")[-1] if "}" in prop.tag else prop.tag
                            # Koordinater
                            for point in prop.iter():
                                ptag = point.tag.split("}")[-1] if "}" in point.tag else point.tag
                                if ptag in ("coordinates", "pos", "posList") and point.text:
                                    coords = point.text.strip().replace(",", " ").split()
                                    if len(coords) >= 2:
                                        try:
                                            geom_coords = (float(coords[1]), float(coords[0]))
                                        except ValueError:
                                            pass
                            if prop.text and prop.text.strip():
                                props[pname] = prop.text.strip()

                        features.append({"properties": props, "_coords": geom_coords})
            used_format = "GML"
        except Exception as e:
            print(f"  [FEIL] {label} WFS: {e}")
            return

    print(f"  {label}: {len(features)} features hentet ({used_format})")

    added = 0
    for feat in features:
        if used_format == "GeoJSON":
            props = feat.get("properties") or {}
            geom  = feat.get("geometry") or {}
            coords = geom.get("coordinates", [None, None])
            lat = coords[1] if len(coords) > 1 else None
            lon = coords[0] if len(coords) > 0 else None
        else:
            props = feat.get("properties") or {}
            coords = feat.get("_coords")
            lat = coords[0] if coords else None
            lon = coords[1] if coords else None

        name = None
        for f in name_fields:
            # Søk case-insensitivt
            for k, v in props.items():
                if k.lower() == f.lower() and v and str(v).strip() not in ("", "None", "nan"):
                    name = str(v).strip()
                    break
            if name:
                break

        inst_type = "Unknown"
        if type_field:
            for k, v in props.items():
                if k.lower() == type_field.lower() and v:
                    inst_type = str(v).strip()
                    break

        entry = {
            "installation_name": name,
            "country": country,
            "region": region,
            "installation_type": inst_type,
            "operator": "Unknown",
            "status": "Unknown",
            "year_installed": None,
            "latitude": lat,
            "longitude": lon,
            "source_url": wfs_url,
            "source_authority": source_authority or label,
            "confidence_level": "High",
        }
        global_inventory.append(entry)
        added += 1

    print(f"  -> {added} installasjoner fra {label} lagt til.")


# ══════════════════════════════════════════════════════════════════════════════
# LANDSPESIFIKKE FUNKSJONER
# ══════════════════════════════════════════════════════════════════════════════

def fetch_norway_sodir():
    arcgis_query_all(
        base_url=(
            "https://factmaps.sodir.no/api/rest/services/Factmaps/"
            "FactMapsWGS84/MapServer/307/query"
        ),
        label="SODIR (Norge)",
        country="Norway",
        region="Norwegian Continental Shelf",
        name_fields=["fclName", "name", "NAVN"],
        type_field="fclKind",
        operator_field="fclCurrentOperatorName",
        status_field="fclPhase",
        source_authority="Regulator (SODIR)",
    )


def fetch_uk_nsta():
    # Lag 0 = Surface Points (plattformer, FPSOs)
    # Lag 1 = Subsea Points (wellheads, manifolds)
    # Feltnavn bekreftet: NAME, INF_TYPE, REP_GROUP (fra NSTA ArcGIS REST dokumentasjon)
    for layer_id, layer_label in [(0, "Surface"), (1, "Subsea")]:
        url = (
            f"https://data.nstauthority.co.uk/arcgis/rest/services/"
            f"Public_WGS84/UKCS_Offshore_Infrastructure_WGS84/"
            f"FeatureServer/{layer_id}/query"
        )
        print(f"Kobler til NSTA UK ({layer_label})...")
        try:
            # Test DNS-oppslag først
            import socket
            socket.getaddrinfo("data.nstauthority.co.uk", 443)
        except OSError:
            print(f"  [DNS-FEIL] Kan ikke nå data.nstauthority.co.uk.")
            print(f"  Dette er trolig en nettverksrestriksjon (VPN/bedriftsbrannmur).")
            print(f"  Løsning: Koble fra VPN og kjør skriptet på nytt, ELLER")
            print(f"  last ned manuelt fra: https://opendata-nstauthority.hub.arcgis.com")
            print(f"  -> 0 installasjoner fra NSTA UK ({layer_label}) lagt til.")
            continue

        arcgis_query_all(
            base_url=url,
            label=f"NSTA UK ({layer_label})",
            country="United Kingdom",
            region="UK Continental Shelf",
            name_fields=["NAME", "Name", "InfrastructureName", "INFRA_NAME",
                         "FACILITY_NAME", "InstallationName", "COMPLEX_NAME"],
            type_field="INF_TYPE",
            operator_field="REP_GROUP",
            status_field="Status",
            source_authority="Regulator (NSTA)",
        )


def fetch_denmark_ens():
    """
    Danish Energy Agency (ENS) via GEUS WFS.
    First fetches the schema to auto-detect field names, then fetches features.
    """
    WFS_URL = "https://data.geus.dk/geusmap/ows/4326.jsp"
    TYPENAME = "ens_platform"
    print(f"Kobler til ENS (Danmark) via WFS...")

    # ── Step 1: hent schema for å finne feltnavnene ──────────────────────────
    field_names = []
    try:
        r = requests.get(WFS_URL, params={
            "service": "WFS", "version": "2.0.0",
            "request": "DescribeFeatureType", "typeName": TYPENAME,
        }, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for el in root.iter():
                tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
                if tag in ("element", "Element"):
                    nm = el.get("name") or el.get("Name")
                    if nm:
                        field_names.append(nm)
            if field_names:
                print(f"  ENS feltnavn funnet: {field_names}")
    except Exception as e:
        print(f"  [ADVARSEL] DescribeFeatureType feilet: {e}")

    # ── Step 2: velg feltmapping ─────────────────────────────────────────────
    # Prøv automatisk oppdagede felt, fall tilbake til kjente kandidater
    all_fields_lower = {f.lower(): f for f in field_names}

    def pick_field(*candidates):
        for c in candidates:
            if c.lower() in all_fields_lower:
                return all_fields_lower[c.lower()]
        return None

    name_field   = pick_field("platform_name", "name", "navn", "label", "installationname")
    type_field   = pick_field("platform_type", "type", "type_code", "kind", "category")
    status_field = pick_field("status", "driftstatus", "status_code", "phase")
    op_field     = pick_field("company_name", "company", "operator", "licensee", "owner")

    print(f"  Felt: navn={name_field}, type={type_field}, status={status_field}, operatør={op_field}")

    # ── Step 3: hent features ────────────────────────────────────────────────
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeName": TYPENAME, "outputFormat": "application/json", "srsName": "EPSG:4326",
    }
    features = []
    try:
        r = requests.get(WFS_URL, params=params, headers=HEADERS, timeout=90)
        if r.status_code == 200 and "features" in r.text[:500]:
            data = r.json()
            features = data.get("features", [])
            print(f"  ENS (Danmark): {len(features)} features hentet (GeoJSON)")
        else:
            print(f"  [ADVARSEL] ENS GeoJSON feilet (HTTP {r.status_code}), prøver GML...")
    except Exception as e:
        print(f"  [FEIL] ENS GeoJSON: {e}")

    if not features:
        print("  [FEIL] Ingen data hentet fra ENS (Danmark).")
        return

    # Debug: vis felt i første feature
    if features:
        sample_props = features[0].get("properties") or {}
        print(f"  Eksempel-felt i første feature: {list(sample_props.keys())[:15]}")
        # Oppdater feltmapping basert på faktiske data om schema-henting feilet
        if not field_names:
            all_fields_lower = {k.lower(): k for k in sample_props.keys()}
            name_field   = pick_field("platform_name","name","navn","label","installationname")
            type_field   = pick_field("platform_type","type","type_code","kind","category")
            status_field = pick_field("status","driftstatus","status_code","phase")
            op_field     = pick_field("company_name","company","operator","licensee","owner")

    added = 0
    for feat in features:
        props = feat.get("properties") or {}
        geom  = feat.get("geometry") or {}
        coords = geom.get("coordinates", [None, None])
        lat = coords[1] if len(coords) > 1 else None
        lon = coords[0] if len(coords) > 0 else None

        name = str(props.get(name_field, "")).strip() if name_field else None
        if not name or name in ("", "None", "nan", "NULL"):
            # fallback: prøv alle felt
            for k, v in props.items():
                if v and str(v).strip() not in ("", "None", "nan") and len(str(v)) > 2:
                    if any(c.isalpha() for c in str(v)):
                        name = str(v).strip()
                        break

        inst_type = str(props.get(type_field, "Unknown")).strip() if type_field else "Unknown"
        status    = str(props.get(status_field, "Unknown")).strip() if status_field else "Unknown"
        operator  = str(props.get(op_field, "Unknown")).strip() if op_field else "Unknown"

        global_inventory.append({
            "installation_name": name,
            "country": "Denmark",
            "region": "Danish Continental Shelf",
            "installation_type": inst_type if inst_type not in ("", "None", "nan") else "Unknown",
            "operator": operator if operator not in ("", "None", "nan") else "Unknown",
            "status": status if status not in ("", "None", "nan") else "Unknown",
            "year_installed": None,
            "latitude": lat,
            "longitude": lon,
            "source_url": WFS_URL,
            "source_authority": "Regulator (Danish Energy Agency / ENS)",
            "confidence_level": "High",
        })
        added += 1

    print(f"  -> {added} installasjoner fra ENS (Danmark) lagt til.")


def fetch_emodnet_europe():
    """
    EMODnet Human Activities — Offshore Oil & Gas Installations.
    Dekker: Danmark, UK, Norge, Nederland, og mange andre europeiske land.
    Harmoniserte felt: navn, land, operatør, produksjonsstart, status, kategori.
    Kilde: https://ows.emodnet-humanactivities.eu/wfs
    """
    WFS_URL = "https://ows.emodnet-humanactivities.eu/wfs"
    print("\nKobler til EMODnet Human Activities (EU offshore installasjoner)...")

    # ── Finn riktig typename via GetCapabilities ──────────────────────────────
    typename = None
    try:
        r = requests.get(WFS_URL, params={
            "service": "WFS", "version": "2.0.0", "request": "GetCapabilities",
        }, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            for el in root.iter():
                tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
                if tag == "Name":
                    val = (el.text or "").strip()
                    if "install" in val.lower() or "offshore" in val.lower() or "oil" in val.lower():
                        typename = val
                        print(f"  EMODnet typename funnet: {typename}")
                        break
    except Exception as e:
        print(f"  [ADVARSEL] GetCapabilities feilet: {e}")

    # Fallback typenames basert på kjent EMODnet navnekonvensjon
    if not typename:
        for candidate in [
            "humanactivities:offshore_installations",
            "humanactivities:EOIL_GAS_OFFSHORE_INSTALLATIONS",
            "humanactivities:oilgas_installations",
            "humanactivities:oil_gas_offshore_installations",
            "offshore_installations",
        ]:
            print(f"  Prøver typename: {candidate}")
            try:
                r = requests.get(WFS_URL, params={
                    "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                    "typeName": candidate, "outputFormat": "application/json",
                    "count": "1",
                }, headers=HEADERS, timeout=20)
                if r.status_code == 200 and "features" in r.text[:200]:
                    typename = candidate
                    print(f"  EMODnet typename bekreftet: {typename}")
                    break
            except Exception:
                pass

    if not typename:
        print("  [FEIL] Kunne ikke finne EMODnet typename. Hopper over EMODnet.")
        return

    # ── Hent alle features ────────────────────────────────────────────────────
    features = []
    try:
        r = requests.get(WFS_URL, params={
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeName": typename, "outputFormat": "application/json",
            "srsName": "EPSG:4326",
        }, headers=HEADERS, timeout=120)
        if r.status_code == 200 and "features" in r.text[:500]:
            data = r.json()
            features = data.get("features", [])
            print(f"  EMODnet: {len(features)} features hentet")
        else:
            print(f"  [FEIL] EMODnet GetFeature feilet (HTTP {r.status_code})")
            return
    except Exception as e:
        print(f"  [FEIL] EMODnet: {e}")
        return

    if not features:
        print("  [FEIL] Ingen EMODnet-data mottatt.")
        return

    # Debug: vis felt
    sample = features[0].get("properties") or {}
    print(f"  Eksempel-felt: {list(sample.keys())[:20]}")

    # ── Feltmapping (EMODnet harmoniserte felt) ───────────────────────────────
    all_lower = {k.lower(): k for k in sample.keys()}

    def pick(*cands):
        for c in cands:
            if c.lower() in all_lower:
                return all_lower[c.lower()]
        return None

    name_f    = pick("name","installation_name","namn","nombre")
    country_f = pick("country","land","pays","country_code")
    type_f    = pick("category","type","installation_type","function","kind")
    status_f  = pick("status","current_status","activity_status","phase")
    op_f      = pick("operator","company","company_name","operateur")
    year_f    = pick("production_start","prod_year","year","year_installed","start_year")

    print(f"  Felt: navn={name_f} land={country_f} type={type_f} status={status_f} op={op_f} år={year_f}")

    # ── Bygg inventory — bare europeiske land vi ikke allerede dekker ────────
    target_countries = {
        "dk": "Denmark", "denmark": "Denmark",
        "gb": "United Kingdom", "uk": "United Kingdom", "united kingdom": "United Kingdom",
    }

    added_by_country = {}
    for feat in features:
        props = feat.get("properties") or {}
        geom  = feat.get("geometry") or {}
        coords = geom.get("coordinates", [None, None])
        lat = coords[1] if geom.get("type") == "Point" and len(coords) > 1 else None
        lon = coords[0] if geom.get("type") == "Point" and len(coords) > 0 else None

        raw_country = str(props.get(country_f, "")).strip().lower() if country_f else ""
        country = target_countries.get(raw_country)
        if not country:
            continue  # bare hent land vi trenger

        name = str(props.get(name_f, "")).strip() if name_f else None
        if not name or name in ("", "None", "nan"):
            continue

        inst_type = str(props.get(type_f, "Unknown")).strip() if type_f else "Unknown"
        status    = str(props.get(status_f, "Unknown")).strip() if status_f else "Unknown"
        operator  = str(props.get(op_f, "Unknown")).strip() if op_f else "Unknown"
        year_raw  = props.get(year_f) if year_f else None
        try:
            year = int(year_raw) if year_raw and str(year_raw).isdigit() else None
        except Exception:
            year = None

        region_map = {
            "Denmark": "Danish Continental Shelf",
            "United Kingdom": "UK Continental Shelf",
        }

        global_inventory.append({
            "installation_name": name,
            "country": country,
            "region": region_map.get(country, country),
            "installation_type": inst_type if inst_type not in ("", "None", "nan") else "Unknown",
            "operator": operator if operator not in ("", "None", "nan") else "Unknown",
            "status": status if status not in ("", "None", "nan") else "Unknown",
            "year_installed": year,
            "latitude": lat,
            "longitude": lon,
            "source_url": WFS_URL,
            "source_authority": "EMODnet Human Activities (EU)",
            "confidence_level": "High",
        })
        added_by_country[country] = added_by_country.get(country, 0) + 1

    for country, n in sorted(added_by_country.items()):
        print(f"  -> {n} installasjoner fra {country} (EMODnet) lagt til.")
    if not added_by_country:
        print("  [INFO] Ingen DK/UK-installasjoner funnet i EMODnet-data.")


def fetch_netherlands_nlog():
    # Bekreftet lagnavn fra GetCapabilities (kjørt 16. april 2026):
    # nlog:GDW_NG_FACILITY_UTM — fasiliteter/plattformer
    base = "https://www.gdngeoservices.nl/geoserver/nlog/ows"
    wfs_query(
        wfs_url=base,
        typename="nlog:GDW_NG_FACILITY_UTM",
        label="NLOG (Nederland)",
        country="Netherlands",
        region="Dutch Continental Shelf / Onshore NL",
        name_fields=["NAAM", "naam", "NAME", "name", "FACILITY_NAME",
                     "label", "OMSCHRIJVING", "omschrijving"],
        type_field="TYPE",
        source_authority="Regulator (EZK/NLOG)",
    )


# ══════════════════════════════════════════════════════════════════════════════
# USA: BSEE (Gulf of Mexico) — ZIP/fixed-width
# ══════════════════════════════════════════════════════════════════════════════

def _read_bsee_zip(url, label):
    print(f"  Laster ned {label} fra BSEE...")
    r = requests.get(url, headers=HEADERS, timeout=120)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    txt_files = [n for n in z.namelist() if n.lower().endswith((".txt", ".dat", ".csv"))]
    if not txt_files:
        raise ValueError(f"Ingen tekstfiler i {label}")
    raw = z.read(txt_files[0])
    return raw.decode("latin-1", errors="replace")


def _find_name_col_heuristic(df):
    """
    Finner kolonnen med plattformnavn i en BSEE fast-bredde-fil
    ved å se etter kolonner med tekstverdier som ligner stedsnavn
    (f.eks. 'SOUTH PASS', 'GRAND ISLE', 'EUGENE ISLAND').
    """
    import re
    best_col, best_score = None, 0
    for col in df.columns:
        vals = df[col].dropna().astype(str).str.strip()
        vals = vals[vals != ""]
        if len(vals) == 0:
            continue
        # Godkjent: inneholder minst 2 bokstaver, lengde 3-30, ikke bare tall/dato
        name_like = vals[
            (vals.str.len().between(3, 30)) &
            (vals.str.contains(r"[A-Za-z]{2,}", regex=True)) &
            (~vals.str.match(r"^\d+$")) &
            (~vals.str.match(r"^\d{2}-[A-Z]{3}-\d{4}$"))
        ]
        score = len(name_like) / len(vals)
        if score > best_score:
            best_score = score
            best_col = col
    return best_col, best_score


def fetch_usa_bsee():
    print("\nKobler til BSEE (USA – Gulf of Mexico)...")

    struct_url = "https://www.data.bsee.gov/Platform/Files/platstrufixed.zip"
    loc_url    = "https://www.data.bsee.gov/Platform/Files/platlocfixed.zip"

    # ── Les strukturfil (ekte fast bredde — bruk read_fwf) ───────────────────
    try:
        struct_text = _read_bsee_zip(struct_url, "Platform Structures")
    except Exception as e:
        print(f"  [FEIL] Strukturfil: {e}")
        return

    try:
        struct_df = pd.read_fwf(
            io.StringIO(struct_text),
            header=None,
            infer_nrows=500,
            dtype=str,
        )
        struct_df = struct_df.astype(str).replace("nan", "")
        print(f"  Strukturfil: {len(struct_df)} rader, {len(struct_df.columns)} kolonner")
        print(f"  Første rad: {list(struct_df.iloc[0])[:8]}")
    except Exception as e:
        print(f"  [FEIL] Klarte ikke lese strukturfil: {e}")
        return

    # Finn navnekolonne heuristisk
    name_col, name_score = _find_name_col_heuristic(struct_df)
    print(f"  Antatt navnekolonne: {name_col} (score {name_score:.2f})")
    if name_col is None or name_score < 0.3:
        print("  [FEIL] Fant ikke navnekolonne. Viser første 3 rader for debugging:")
        print(struct_df.head(3).to_string())
        return

    # ── Les lokasjonsfil ──────────────────────────────────────────────────────
    coord_lookup = {}
    try:
        loc_text = _read_bsee_zip(loc_url, "Platform Locations")
        loc_df = pd.read_fwf(io.StringIO(loc_text), header=None, infer_nrows=500, dtype=str)
        loc_df = loc_df.astype(str).replace("nan", "")
        print(f"  Lokasjonsfil: {len(loc_df)} rader, {len(loc_df.columns)} kolonner")

        # Finn lat/lon-kolonner: desimaltall mellom -180 og 180
        import re as _re
        lat_col = lon_col = id_col = None
        for col in loc_df.columns:
            sample = loc_df[col].dropna().astype(str).head(20)
            numeric = [v for v in sample if _re.match(r"^-?\d+\.\d+$", v.strip())]
            if len(numeric) >= 5:
                floats = [float(v) for v in numeric]
                avg = sum(floats) / len(floats)
                if 15 < avg < 35 and lat_col is None:
                    lat_col = col
                elif -100 < avg < -80 and lon_col is None:
                    lon_col = col
        if lat_col and lon_col:
            print(f"  Lat-kolonne: {lat_col}, Lon-kolonne: {lon_col}")
    except Exception as e:
        print(f"  [ADVARSEL] Lokasjonsfil ikke tilgjengelig: {e}")
        loc_df = None

    # ── Bygg inventory ────────────────────────────────────────────────────────
    added = 0
    for idx, row in struct_df.iterrows():
        name = str(row[name_col]).strip()
        if not name or name in ("nan", ""):
            continue

        lat = lon = None
        if loc_df is not None and lat_col and lon_col and idx < len(loc_df):
            try:
                lat = float(loc_df.iloc[idx][lat_col])
                lon = float(loc_df.iloc[idx][lon_col])
            except Exception:
                pass

        entry = {
            "installation_name": name,
            "country": "United States",
            "region": "Gulf of Mexico",
            "installation_type": "Platform/Structure",
            "operator": "Se BSEE operatørtabell",
            "status": "Unknown",
            "year_installed": None,
            "latitude": lat,
            "longitude": lon,
            "source_url": struct_url,
            "source_authority": "Regulator (BSEE)",
            "confidence_level": "High",
        }
        global_inventory.append(entry)
        added += 1

    print(f"  -> {added} BSEE-strukturer lagt til.")


# ══════════════════════════════════════════════════════════════════════════════
# EKSTRAKSJON
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 60)
print("  GLOBAL OFFSHORE INVENTORY — DATAEKSTRAKSJON v4")
print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
print("=" * 60)

fetch_norway_sodir()
fetch_uk_nsta()        # prøver data.nstauthority.co.uk — kan blokkeres av VPN
fetch_emodnet_europe() # EMODnet dekker DK + UK med harmoniserte felt
fetch_denmark_ens()    # GEUS WFS — direkte ENS-data for Danmark
fetch_netherlands_nlog()
fetch_usa_bsee()

# ── Validering ────────────────────────────────────────────────────────────────
valid_inventory = []
for item in global_inventory:
    name = item.get("installation_name")
    if not name or str(name).strip().upper() in ("", "UNKNOWN", "NAN", "NONE", "NULL", "<NULL>"):
        missing_data_log.append(item)
    else:
        valid_inventory.append(item)

# ── Eksport ───────────────────────────────────────────────────────────────────
by_country = {}
for item in valid_inventory:
    c = item.get("country", "Unknown")
    by_country[c] = by_country.get(c, 0) + 1

output_payload = {
    "system_metadata": {
        "extraction_date": datetime.now(timezone.utc).isoformat(),
        "sources": [
            "SODIR Norway — ArcGIS REST (MapServer/307)",
            "NSTA UK — ArcGIS FeatureServer (UKCS_Offshore_Infrastructure_WGS84)",
            "EMODnet Human Activities — WFS offshore installations (DK + UK)",
            "Danish Energy Agency — WFS (data.geus.dk, ens_platform)",
            "NLOG Netherlands — WFS (gdngeoservices.nl)",
            "BSEE USA — ASCII bulk download (platstrufixed.zip)",
        ],
        "total_valid_records": len(valid_inventory),
        "total_unverified_records": len(missing_data_log),
        "breakdown_by_country": by_country,
    },
    "inventory": valid_inventory,
    "missing_data_log": missing_data_log,
}

script_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(script_dir, "global_offshore_inventory_local.json")

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(output_payload, f, indent=4, ensure_ascii=False)

print()
print("=" * 60)
print(f"  SUKSESS! Lagret til: {output_path}")
print()
for country, count in sorted(by_country.items()):
    print(f"  {country:<30} {count:>6} installasjoner")
print(f"  {'─' * 38}")
print(f"  {'TOTALT':<30} {len(valid_inventory):>6}")
print(f"  {'Mangelfulle (log)':<30} {len(missing_data_log):>6}")
print("=" * 60)
print()
print("Åpne i Excel: Data → Hent data → Fra JSON → velg filen")
