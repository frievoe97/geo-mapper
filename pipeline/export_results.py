"""Final step: export CSV results for the selected geodata source."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import List, Set

import pandas as pd
from .storage import (
    get_selections,
    get_geodata_frames,
    get_geodata_mappings,
    get_export_geodata_source,
    get_meta_config,
)


logger = logging.getLogger(__name__)

RESULTS_ROOT = Path("results")


def _write_mapped_pairs(
    base_dir: Path,
    dataframe: pd.DataFrame,
    selected_sources: Set[str],
    id_col: str | None,
    name_col: str | None,
    value_cols: list[str],
) -> Path:
    """Write CSV with mapped pairs: original ID/name, geodata ID/name,
    mapper/parameter, followed by any configured value columns.

    For each selected geodata CSV we keep a separate mapping. This means a
    single input row can appear multiple times in the output (once per CSV
    where it was mapped).
    """
    base_dir.mkdir(parents=True, exist_ok=True)

    mappings_by_source = get_geodata_mappings()
    rows: list[dict[str, object]] = []

    # If no source was explicitly selected, use all sources that have mappings
    effective_sources: Set[str]
    if selected_sources:
        effective_sources = selected_sources
    else:
        effective_sources = set(mappings_by_source.keys())

    for source_path, mapping_df in mappings_by_source.items():
        if source_path not in effective_sources:
            continue
        if "mapped_value" not in mapping_df.columns:
            continue
        mask = mapping_df["mapped_value"].notna()
        if not mask.any():
            continue
        # For each mapped row in this CSV:
        # write original ID/name (if present), value columns, and geodata ID/name
        for idx in mapping_df.index[mask]:
            original_id = dataframe.loc[idx, id_col] if id_col and id_col in dataframe.columns else pd.NA
            original_name = dataframe.loc[idx, name_col] if name_col and name_col in dataframe.columns else pd.NA
            geodata_id = mapping_df.loc[idx, "mapped_value"]
            row: dict[str, object] = {
                "original_id": original_id,
                "original_name": original_name,
                "geodata_id": geodata_id,
                "geodata_name": mapping_df.loc[idx, "mapped_label"],
            }
            # Append value columns, preserving the original header names
            for col in value_cols:
                if col in dataframe.columns:
                    row[col] = dataframe.loc[idx, col]
                else:
                    row[col] = pd.NA
            row["mapper"] = mapping_df.loc[idx, "mapped_by"]
            row["parameter"] = mapping_df.loc[idx, "mapped_param"]
            rows.append(row)

    if rows:
        base_cols = [
            "original_id",
            "original_name",
            "geodata_id",
            "geodata_name",
            "mapper",
            "parameter",
        ]
        export_cols = base_cols + list(value_cols)
        out_df = pd.DataFrame(rows, columns=export_cols)
        # Mapper order in the export: first id_exact, then unique_name, ...
        mapper_priority = {
            "id_exact": 0,
            "unique_name": 1,
            "token_permutation": 2,
            "regex_replace": 3,
            "suffix_variants": 4,
            "fuzzy_confident": 5,
        }
        out_df["_mapper_order"] = out_df["mapper"].map(mapper_priority).fillna(len(mapper_priority))
        out_df = out_df.sort_values("_mapper_order", kind="stable").drop(columns=["_mapper_order"])
    else:
        base_cols = [
            "original_id",
            "original_name",
            "geodata_id",
            "geodata_name",
            "mapper",
            "parameter",
        ]
        export_cols = base_cols + list(value_cols)
        out_df = pd.DataFrame(columns=export_cols)
    out_path = base_dir / "mapped_pairs.csv"
    out_df.to_csv(out_path, index=False)
    return out_path


def _write_unmapped_original(
    base_dir: Path,
    dataframe: pd.DataFrame,
    selected_sources: Set[str],
    id_col: str | None,
    name_col: str | None,
) -> Path:
    """Write CSV with original ID/name values that were not mapped by any selected CSV."""
    base_dir.mkdir(parents=True, exist_ok=True)

    mappings_by_source = get_geodata_mappings()
    # If no source was explicitly selected, use all known sources.
    if selected_sources:
        effective_sources = selected_sources
    else:
        effective_sources = set(mappings_by_source.keys())

    mapped_indices: set[object] = set()
    for source_path, mapping_df in mappings_by_source.items():
        if source_path not in effective_sources:
            continue
        if "mapped_value" not in mapping_df.columns:
            continue
        mask = mapping_df["mapped_value"].notna()
        mapped_indices.update(mapping_df.index[mask])

    # Build the list of unmapped original rows explicitly so that index
    # alignment cannot accidentally change exported values.
    rows: list[dict[str, object]] = []
    for idx in dataframe.index:
        if idx in mapped_indices:
            continue
        if id_col and id_col in dataframe.columns:
            original_id = dataframe.at[idx, id_col]
        else:
            original_id = pd.NA
        if name_col and name_col in dataframe.columns:
            original_name = dataframe.at[idx, name_col]
        else:
            original_name = pd.NA
        rows.append(
            {
                "original_id": original_id,
                "original_name": original_name,
            }
        )

    if rows:
        out_df = pd.DataFrame(rows, columns=["original_id", "original_name"])
    else:
        out_df = pd.DataFrame(columns=["original_id", "original_name"])
    out_path = base_dir / "unmapped_orginal.csv"
    out_df.to_csv(out_path, index=False)
    return out_path


def _write_unmapped_geodata(
    base_dir: Path, dataframe: pd.DataFrame, selected_sources: Set[str]
) -> Path:
    """Write CSV with geodata rows that were never used in mappings."""
    base_dir.mkdir(parents=True, exist_ok=True)

    # Map (geodata_source_path) -> set of used geodata IDs (as strings),
    # based on the per-CSV mapping DataFrames.
    mappings_by_source = get_geodata_mappings()
    used_ids_by_source: dict[str, set[str]] = {}
    for source_path, mapping_df in mappings_by_source.items():
        if "mapped_value" not in mapping_df.columns:
            continue
        mask = mapping_df["mapped_value"].notna()
        if not mask.any():
            continue
        ids = {str(v) for v in mapping_df.loc[mask, "mapped_value"].dropna().astype(str)}
        if ids:
            used_ids_by_source[str(source_path)] = ids

    geodata_frames = get_geodata_frames()
    rows: List[dict[str, str]] = []
    consider_all = not selected_sources

    for path, frame in geodata_frames:
        source_path = str(path)
        if not consider_all and source_path not in selected_sources:
            continue
        if not {"id", "name"}.issubset(frame.columns):
            continue
        used_ids = used_ids_by_source.get(source_path, set())
        for gid, name in zip(frame["id"], frame["name"], strict=False):
            if pd.isna(gid):
                continue
            gid_str = str(gid)
            if gid_str in used_ids:
                continue
            rows.append(
                {
                    "geodata_id": gid_str,
                    "geodata_name": str(name),
                }
            )

    out_df = pd.DataFrame(rows, columns=["geodata_id", "geodata_name"])
    out_path = base_dir / "unmapped_geodata.csv"
    out_df.to_csv(out_path, index=False)
    return out_path


def _write_meta_json(base_dir: Path) -> Path | None:
    """Write a meta.json with column/level/year info based on selections.

    - Start from any existing meta_config (from an input JSON, if provided)
    - Fill/fix fields: id_column, name_column, value_columns, geodata_level, geodata_year
    - Never modify the original input JSON file; this only writes into results/.
    """
    selections = get_selections()
    meta = get_meta_config()
    if not isinstance(meta, dict):
        meta = {}

    # Try to infer dataset type/level/year from the export path layout:
    # results/[INPUT_NAME]/[NUTS/LAU]/[level]/[year]/
    parsed_dataset: str | None = None
    parsed_level: str | None = None
    parsed_year: str | None = None
    parts = base_dir.parts
    # Expected structure has at least 5 segments, e.g. ("results","data_matrix","NUTS","3","2024")
    if len(parts) >= 5 and parts[-5] == RESULTS_ROOT.name:
        dataset_dir = parts[-3]
        level_dir = parts[-2]
        version_dir = parts[-1]
        parsed_year = version_dir
        raw_dataset = dataset_dir.upper()
        if raw_dataset == "LAU":
            parsed_dataset = "LAU"
            parsed_level = "LAU"
        elif raw_dataset == "NUTS":
            parsed_dataset = "NUTS"
            parsed_level = f"NUTS {level_dir}"

    # Columns
    if "id_column" not in meta:
        meta["id_column"] = selections.id_column
    if "name_column" not in meta:
        meta["name_column"] = selections.name_column
    if "value_columns" not in meta:
        value_cols = getattr(selections, "value_columns", None) or []
        meta["value_columns"] = list(value_cols)

    # Level (administrative level) -> geodata_level
    if "geodata_level" not in meta:
        level_value: str | None = None
        # Prefer to derive from the export path (it reflects the actual output)
        if parsed_level is not None:
            level_value = parsed_level
        else:
            if selections.geodata_type == "LAU":
                level_value = "LAU"
            elif (
                selections.geodata_type == "NUTS"
                and selections.nuts_level is not None
                and selections.nuts_level != "unknown"
            ):
                level_value = f"NUTS {selections.nuts_level}"
        meta["geodata_level"] = level_value if level_value is not None else "unknown"

    # Year / version -> geodata_year
    # If possible, take the year from the export path – this is always the
    # actual exported year, even if "unknown" was selected earlier.
    if "geodata_year" not in meta:
        year_value: str | None = parsed_year
        if not year_value:
            year_value = selections.geodata_version
        if year_value in (None, "unknown"):
            year_value = "unknown"
        meta["geodata_year"] = year_value

    try:
        base_dir.mkdir(parents=True, exist_ok=True)
        out_path = base_dir / "meta.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        logger.info("Wrote meta-information to %s.", out_path)
        return out_path
    except Exception as exc:
        logger.warning("Could not write meta.json: %s", exc)
        return None


def export_results_step(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Export CSVs for the previously selected geodata source."""
    selected_source = get_export_geodata_source()
    if not selected_source:
        logger.info(
            "No geodata source selected for export – skipping export step.",
        )
        return dataframe

    selected_sources: Set[str] = {selected_source}

    selections = get_selections()
    id_col = selections.id_column
    name_col = (
        selections.name_column
        or selections.column
        or (dataframe.columns[0] if len(dataframe.columns) else None)
    )
    value_cols = [c for c in getattr(selections, "value_columns", []) if c in dataframe.columns]
    input_name = selections.input_name or "input"

    parts: list[str] = []
    if selections.geodata_type and selections.geodata_type != "unknown":
        parts.append(selections.geodata_type)
    if selections.nuts_level and selections.nuts_level != "unknown":
        parts.append(f"level_{selections.nuts_level}")
    if selections.geodata_version and selections.geodata_version != "unknown":
        parts.append(selections.geodata_version)
    geodata_name = "_".join(parts) if parts else "geodata"

    # Target path: results/[INPUT_NAME]/[NUTS/LAU]/[level]/[year]/
    # Try to derive dataset type/level/year from the selected geodata CSV path.
    try:
        src_path = Path(selected_source)
        version_dir = src_path.parent.name  # e.g. "2013"
        dataset_dir_raw = src_path.parent.parent.name  # e.g. "nuts", "lau", "NUTS_0", "LAU"
        raw_upper = dataset_dir_raw.upper()
        # New layout: NUTS_<level>/[year]/file or LAU/[year]/file
        m = re.match(r"(NUTS)_(\d+)$", raw_upper)
        if m:
            dataset_dir = m.group(1)  # "NUTS"
            level_dir = m.group(2)    # "0"–"3"
        else:
            dataset_dir = raw_upper   # "LAU" or similar
            level_match = re.search(r"_level_(\d+)\.csv$", src_path.name)
            if level_match:
                level_dir = level_match.group(1)
            else:
                # Fallback: treat dataset name as the level
                level_dir = dataset_dir
        base_dir = RESULTS_ROOT / input_name / dataset_dir / level_dir / version_dir
    except Exception:
        # Fallback: keep using the previously composed geodata_name
        base_dir = RESULTS_ROOT / input_name / geodata_name

    mapped_path = _write_mapped_pairs(
        base_dir,
        dataframe,
        selected_sources,
        id_col,
        name_col,
        value_cols,
    )
    unmapped_input_path = _write_unmapped_original(
        base_dir,
        dataframe,
        selected_sources,
        id_col,
        name_col,
    )
    unmapped_geodata_path = _write_unmapped_geodata(base_dir, dataframe, selected_sources)
    _write_meta_json(base_dir)

    logger.info("Exported mapped pairs to: %s", mapped_path)
    logger.info("Exported unmapped original values to: %s", unmapped_input_path)
    logger.info("Exported unused geodata rows to: %s", unmapped_geodata_path)

    return dataframe
