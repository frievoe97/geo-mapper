#!/usr/bin/env python3
import logging
import json
import re
import csv
import sys
import importlib.util
from pathlib import Path

from pyproj import Transformer
from pyproj.exceptions import ProjError

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from prepare.regionalschluessel import load_regionalschluessel_mapping  # noqa: E402

CONSTANTS_PATH = ROOT_DIR / "geo_mapper" / "pipeline" / "constants.py"


def _load_constants_module():
    spec = importlib.util.spec_from_file_location("pipeline.constants", CONSTANTS_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"pipeline.constants konnte nicht unter {CONSTANTS_PATH} geladen werden")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


constants = _load_constants_module()
GEODATA_RAW_ROOT = constants.GEODATA_RAW_ROOT
GEODATA_CLEAN_ROOT = constants.GEODATA_CLEAN_ROOT
GEOJSON_ROOT = constants.GEOJSON_ROOT
GEODATA_CSV_ROOT = constants.GEODATA_CSV_ROOT

BASE_IN = GEODATA_RAW_ROOT
BASE_OUT = GEODATA_CLEAN_ROOT
GEO_OUT = GEOJSON_ROOT
CSV_OUT = GEODATA_CSV_ROOT
logger = logging.getLogger(__name__)

LAU_IN = BASE_IN / "lau"
NUTS_IN = BASE_IN / "nuts"

LAU_MAPPING = (("id", "GISCO_ID"), ("name", "LAU_NAME"))
NUTS_MAPPING = (("id_nuts", "NUTS_ID"), ("name", "NUTS_NAME"))

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
    except (TypeError, ValueError):
        return False


def transform_coords(coords):
    if isinstance(coords, (list, tuple)) and _is_number_like(coords[0]) and _is_number_like(coords[1]):
        try:
            x = float(coords[0])
            y = float(coords[1])
        except (TypeError, ValueError):
            return coords
        try:
            lon, lat = TRANSFORMER.transform(x, y)
        except ProjError:
            # Keep original coordinates if reprojection fails (projection error)
            return coords
        rest = coords[2:] if len(coords) > 2 else []
        return [lon, lat] + [
            transform_coords(r) if isinstance(r, (list, tuple)) else r for r in rest
        ]
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
    return {"type": t, "coordinates": transform_coords(coords)}


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
        except (TypeError, ValueError):
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



def main():
    if LAU_IN.exists():
        for p in sorted(LAU_IN.glob("*.geojson")):
            process_lau(p)
    if NUTS_IN.exists():
        for p in sorted(NUTS_IN.glob("*.geojson")):
            process_nuts(p)
    add_regionalschluessel_to_nuts_files()


def add_regionalschluessel_to_nuts_files():
    mapping = load_regionalschluessel_mapping(BASE_IN, logger)
    if not mapping:
        return
    missing_ids = set()
    csv_files = sorted(CSV_OUT.glob("NUTS_*/*/*.csv"))
    for csv_path in csv_files:
        try:
            with csv_path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                headers = list(reader.fieldnames or [])
                rows = []
                for row in reader:
                    nuts_id = (row.get("id_nuts") or "").strip()
                    region_value = mapping.get(nuts_id)
                    if region_value is None and nuts_id:
                        missing_ids.add(nuts_id)
                        region_value = ""
                    row["id_ars"] = region_value if region_value is not None else ""
                    rows.append(row)
        except FileNotFoundError:
            continue
        if not rows:
            continue
        if "id_ars" not in headers:
            headers.append("id_ars")
        save_csv(rows, headers, csv_path)
    geo_files = sorted(GEO_OUT.glob("NUTS_*/*/*.geojson"))
    for geo_path in geo_files:
        data = load(geo_path)
        feats = features(data)
        changed = False
        for feat in feats:
            if not isinstance(feat, dict):
                continue
            props = feat.setdefault("properties", {})
            nuts_id = (props.get("id_nuts") or "").strip()
            if not nuts_id:
                continue
            region_value = mapping.get(nuts_id)
            if region_value is None:
                missing_ids.add(nuts_id)
                region_value = ""
            if props.get("id_ars") != region_value:
                props["id_ars"] = region_value
                changed = True
        if changed and isinstance(data, dict):
            save_json(data, geo_path)
        elif changed:
            save_json({"type": "FeatureCollection", "features": feats}, geo_path)


if __name__ == "__main__":
    main()
