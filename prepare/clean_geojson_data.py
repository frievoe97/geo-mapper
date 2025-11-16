#!/usr/bin/env python3
import logging
import json
import re
import csv
from pathlib import Path

from pyproj import Transformer

BASE_IN = Path("geodata_raw")
BASE_OUT = Path("geodata_clean")
GEO_OUT = BASE_OUT / "geojson"
CSV_OUT = BASE_OUT / "csv"
logger = logging.getLogger(__name__)

LAU_IN = BASE_IN / "lau"
NUTS_IN = BASE_IN / "nuts"

LAU_MAPPING = (("id", "GISCO_ID"), ("name", "LAU_NAME"))
NUTS_MAPPING = (("id", "NUTS_ID"), ("name", "NUTS_NAME"))

YEAR_RE = re.compile(r"(\d{4})")
TRANSFORMER = Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)


def load(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(obj, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def save_csv(rows, headers, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)


def year_from_name(name):
    m = YEAR_RE.search(name)
    return m.group(1) if m else None


def features(data):
    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        return data.get("features", [])
    if isinstance(data, dict) and data.get("type") == "Feature":
        return [data]
    if isinstance(data, list):
        return data
    return []


def keep_props(props, mapping):
    out = {}
    for target_key, source_key in mapping:
        v = props.get(source_key) if isinstance(props, dict) else None
        if v is None:
            out[target_key] = ""
            continue
        if source_key == "GISCO_ID":
            s = str(v).strip()
            out[target_key] = s[3:] if len(s) > 3 else ""
        else:
            out[target_key] = v
    return out



def _is_point_like(c):
    return isinstance(c, (list, tuple)) and len(c) >= 2 and isinstance(c[0], (int, float)) or (len(c)>=2 and _is_number_like(c[0]) and _is_number_like(c[1]))

def _is_number_like(x):
    try:
        float(x)
        return True
    except Exception:
        return False


def transform_coords(coords):
    if isinstance(coords, (list, tuple)) and _is_number_like(coords[0]) and _is_number_like(coords[1]):
        try:
            x = float(coords[0]); y = float(coords[1])
            lon, lat = TRANSFORMER.transform(x, y)
            rest = coords[2:] if len(coords) > 2 else []
            return [lon, lat] + [transform_coords(r) if isinstance(r, (list, tuple)) else r for r in rest]
        except Exception:
            return coords
    if isinstance(coords, (list, tuple)):
        return [transform_coords(c) for c in coords]
    return coords


def reproject(geom):
    if not geom or not isinstance(geom, dict):
        return geom
    t = geom.get("type")
    if t == "GeometryCollection":
        geoms = geom.get("geometries", [])
        return {"type": "GeometryCollection", "geometries": [reproject(g) for g in geoms]}
    coords = geom.get("coordinates")
    if coords is None:
        return geom
    try:
        return {"type": t, "coordinates": transform_coords(coords)}
    except Exception:
        return geom


def process_lau(path):
    y = year_from_name(path.name)
    if not y:
        return
    data = load(path)
    feats = features(data)
    out_feats = []
    rows = []
    for f in feats:
        if not isinstance(f, dict):
            continue
        props = f.get("properties") or {}
        if props.get("CNTR_CODE") != "DE":
            continue
        geom = reproject(f.get("geometry"))
        props_filtered = keep_props(props, LAU_MAPPING)
        out_feats.append({"type": "Feature", "properties": props_filtered, "geometry": geom})
        rows.append(props_filtered)
    if not out_feats:
        return
    # New layout: geodata_clean/[csv|geojson]/LAU/[year]/file
    lau_dir = "LAU"
    geo_path = GEO_OUT / lau_dir / y / f"lau_{y}_level_0.geojson"
    csv_path = CSV_OUT / lau_dir / y / f"lau_{y}_level_0.csv"
    save_json({"type": "FeatureCollection", "features": out_feats}, geo_path)
    save_csv(rows, [target for target, _ in LAU_MAPPING], csv_path)
    logger.info("Wrote LAU %s: %d features", y, len(out_feats))


def process_nuts(path):
    y = year_from_name(path.name)
    if not y:
        return
    data = load(path)
    feats = features(data)
    buckets = {0: {"feats": [], "rows": []}, 1: {"feats": [], "rows": []}, 2: {"feats": [], "rows": []}, 3: {"feats": [], "rows": []}}
    for f in feats:
        if not isinstance(f, dict):
            continue
        props = f.get("properties") or {}
        if props.get("CNTR_CODE") != "DE":
            continue
        lvl = props.get("LEVL_CODE")
        try:
            li = int(str(lvl))
        except Exception:
            continue
        if li not in buckets:
            continue
        geom = reproject(f.get("geometry"))
        props_filtered = keep_props(props, NUTS_MAPPING)
        buckets[li]["feats"].append({"type": "Feature", "properties": props_filtered, "geometry": geom})
        buckets[li]["rows"].append(props_filtered)
    for lvl, d in buckets.items():
        if not d["feats"]:
            continue
        # New layout: geodata_clean/[csv|geojson]/NUTS_<level>/[year]/file
        nuts_dir = f"NUTS_{lvl}"
        geo_path = GEO_OUT / nuts_dir / y / f"nuts_{y}_level_{lvl}.geojson"
        csv_path = CSV_OUT / nuts_dir / y / f"nuts_{y}_level_{lvl}.csv"
        save_json({"type": "FeatureCollection", "features": d["feats"]}, geo_path)
        save_csv(d["rows"], [target for target, _ in NUTS_MAPPING], csv_path)
        logger.info("Wrote NUTS %s level %s: %d features", y, lvl, len(d['feats']))


def main():
    if LAU_IN.exists():
        for p in sorted(LAU_IN.glob("*.geojson")):
            process_lau(p)
    if NUTS_IN.exists():
        for p in sorted(NUTS_IN.glob("*.geojson")):
            process_nuts(p)


if __name__ == "__main__":
    main()
