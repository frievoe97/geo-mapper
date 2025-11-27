"""Final step: export CSV results for the selected geodata source."""

from __future__ import annotations
import logging
import re
import shutil
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd

from .storage import (
    get_selections,
    get_geodata_frames,
    get_geodata_mappings,
    get_export_geodata_source,
    get_meta_config,
    get_output_root,
    get_geodata_export_mode,
)
from .constants import (
    RESULTS_ROOT,
    MAPPER_PRIORITY,
    GEODATA_ID_COLUMNS,
    infer_dataset_family,
    GEODATA_CSV_ROOT,
    GEOJSON_ROOT,
    PACKAGE_ROOT,
)


logger = logging.getLogger(__name__)


def _stringify_geodata_id_value(value: object) -> str | None:
    """Convert a geodata ID cell to a stable string representation."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, (int,)):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    text = str(value)
    if not text:
        return None
    return text


def _build_geodata_id_lookup(frame: pd.DataFrame) -> Dict[str, object]:
    """Return mapping from canonical 'id' to row index for a geodata DataFrame."""
    if "id" not in frame.columns:
        return {}
    lookup: Dict[str, object] = {}
    for idx, gid in zip(frame.index, frame["id"], strict=False):
        key = _stringify_geodata_id_value(gid)
        if key is None:
            continue
        lookup[key] = idx
    return lookup


def _collect_geodata_id_values(
    source_path: str,
    geodata_id: object,
    frame_by_source: dict[str, pd.DataFrame],
    dataset_by_source: dict[str, str | None],
    lookup_by_source: dict[str, Dict[str, object]],
) -> dict[str, object]:
    """Return export fields for dataset-specific geodata IDs."""
    dataset = dataset_by_source.get(source_path)
    mapping = GEODATA_ID_COLUMNS.get(dataset or "", [])
    if not mapping:
        return {}
    frame = frame_by_source.get(source_path)
    lookup = lookup_by_source.get(source_path, {})
    key = _stringify_geodata_id_value(geodata_id)
    row_idx = lookup.get(key)
    values: dict[str, object] = {}
    for src_col, export_col in mapping:
        if row_idx is None or frame is None or src_col not in frame.columns:
            values[export_col] = pd.NA
            continue
        val = frame.at[row_idx, src_col]
        parsed = _stringify_geodata_id_value(val)
        values[export_col] = parsed if parsed is not None else pd.NA
    return values


def _write_mapped_pairs(
    base_dir: Path,
    dataframe: pd.DataFrame,
    selected_sources: Set[str],
    id_cols: list[str],
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
    geodata_frames = get_geodata_frames()
    frame_by_source: dict[str, pd.DataFrame] = {str(path): frame for path, frame in geodata_frames}
    dataset_by_source: dict[str, str | None] = {
        source_path: infer_dataset_family(Path(source_path)) for source_path in frame_by_source
    }
    lookup_by_source: dict[str, Dict[str, object]] = {
        source_path: _build_geodata_id_lookup(frame) for source_path, frame in frame_by_source.items()
    }
    rows: list[dict[str, object]] = []

    # If no source was explicitly selected, use all sources that have mappings
    effective_sources: Set[str]
    if selected_sources:
        effective_sources = selected_sources
    else:
        effective_sources = set(mappings_by_source.keys())

    active_extra_cols: List[str] = []
    for source_path in effective_sources:
        dataset = dataset_by_source.get(source_path)
        for _src_col, export_col in GEODATA_ID_COLUMNS.get(dataset or "", []):
            if export_col not in active_extra_cols:
                active_extra_cols.append(export_col)

    # Prepare ID field names (multiple ID columns are numbered in the export).
    id_field_names: list[str] = []
    if id_cols:
        id_field_names = [f"original_id_{i}" for i in range(1, len(id_cols) + 1)]

    for source_path, mapping_df in mappings_by_source.items():
        if source_path not in effective_sources:
            continue
        if "mapped_value" not in mapping_df.columns:
            continue
        mask = mapping_df["mapped_value"].notna()
        if not mask.any():
            continue
        # For each mapped row in this CSV:
        # write original IDs/Namen (if present), value columns, and geodata ID/name
        for idx in mapping_df.index[mask]:
            original_name = dataframe.loc[idx, name_col] if name_col and name_col in dataframe.columns else pd.NA
            geodata_id = mapping_df.loc[idx, "mapped_value"]
            row: dict[str, object] = {}

            # Export multiple ID columns: original_id_1, original_id_2, ...
            if id_cols:
                for field_name, col in zip(id_field_names, id_cols, strict=False):
                    if col in dataframe.columns:
                        row[field_name] = dataframe.loc[idx, col]
                    else:
                        row[field_name] = pd.NA

            row["original_name"] = original_name
            row["geodata_name"] = mapping_df.loc[idx, "mapped_label"]
            extra_ids = _collect_geodata_id_values(
                source_path,
                geodata_id,
                frame_by_source,
                dataset_by_source,
                lookup_by_source,
            )
            row.update(extra_ids)
            for export_col in active_extra_cols:
                row.setdefault(export_col, pd.NA)
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
        base_cols: list[str] = []
        # First the ID columns (numbered if present)
        if id_cols:
            base_cols.extend(id_field_names)

        base_cols.extend(
            [
                "original_name",
                "geodata_name",
                *active_extra_cols,
                "mapper",
                "parameter",
            ]
        )
        filtered_value_cols = [c for c in value_cols if c not in base_cols]
        export_cols = base_cols + filtered_value_cols
        out_df = pd.DataFrame(rows, columns=export_cols)
        # Mapper order in the export mirrors DEFAULT_MAPPERS.
        mapper_priority = MAPPER_PRIORITY
        mapper_order = out_df["mapper"].map(mapper_priority)
        mapper_order = mapper_order.fillna(len(mapper_priority))
        out_df.loc[:, "_mapper_order"] = mapper_order
        out_df = out_df.sort_values("_mapper_order", kind="stable").drop(columns=["_mapper_order"])
    else:
        base_cols = []
        if id_cols:
            base_cols.extend(id_field_names)

        base_cols.extend(
            [
                "original_name",
                "geodata_name",
                *active_extra_cols,
                "mapper",
                "parameter",
            ]
        )
        filtered_value_cols = [c for c in value_cols if c not in base_cols]
        export_cols = base_cols + filtered_value_cols
        out_df = pd.DataFrame(columns=export_cols)
    out_path = base_dir / "mapped_pairs.csv"
    out_df.to_csv(out_path, index=False)
    return out_path


def _write_unmapped_original(
    base_dir: Path,
    dataframe: pd.DataFrame,
    selected_sources: Set[str],
    id_cols: list[str],
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

    id_field_names: list[str] = []
    if id_cols:
        id_field_names = [f"original_id_{i}" for i in range(1, len(id_cols) + 1)]
    for idx in dataframe.index:
        if idx in mapped_indices:
            continue
        row: dict[str, object] = {}

        if id_cols:
            for field_name, col in zip(id_field_names, id_cols, strict=False):
                if col in dataframe.columns:
                    row[field_name] = dataframe.at[idx, col]
                else:
                    row[field_name] = pd.NA

        if name_col and name_col in dataframe.columns:
            original_name = dataframe.at[idx, name_col]
        else:
            original_name = pd.NA
        row["original_name"] = original_name
        rows.append(row)

    base_cols: list[str] = []
    if id_cols:
        base_cols.extend(id_field_names)
    base_cols.append("original_name")

    if rows:
        out_df = pd.DataFrame(rows, columns=base_cols)
    else:
        out_df = pd.DataFrame(columns=base_cols)
    out_path = base_dir / "unmapped_orginal.csv"
    out_df.to_csv(out_path, index=False)
    return out_path


def _write_unmapped_geodata(
    base_dir: Path, selected_sources: Set[str]
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
    rows: List[dict[str, object]] = []
    consider_all = not selected_sources

    for path, frame in geodata_frames:
        source_path = str(path)
        if not consider_all and source_path not in selected_sources:
            continue
        if "name" not in frame.columns:
            continue
        dataset_family = infer_dataset_family(path) or ""
        id_mappings = GEODATA_ID_COLUMNS.get(dataset_family, [])
        used_ids = used_ids_by_source.get(source_path, set())
        # Fallback: use canonical 'id' column for used/unused detection
        if "id" not in frame.columns:
            continue
        for idx, gid in frame["id"].items():
            if pd.isna(gid):
                continue
            gid_str = str(gid)
            if gid_str in used_ids:
                continue
            row: dict[str, object] = {}
            # Export dataset-specific ID columns
            for src_col, export_col in id_mappings:
                if src_col in frame.columns:
                    val = frame.at[idx, src_col]
                else:
                    val = None
                key = _stringify_geodata_id_value(val)
                row[export_col] = key if key is not None else pd.NA
            # Export name column
            name_val = frame.at[idx, "name"]
            row["geodata_name"] = str(name_val)
            rows.append(row)

    # Determine all potential export ID columns (for a consistent column order)
    export_id_cols: list[str] = []
    for mapping_list in GEODATA_ID_COLUMNS.values():
        for _src_col, export_col in mapping_list:
            if export_col not in export_id_cols:
                export_id_cols.append(export_col)

    base_cols = [col for col in export_id_cols if any(col in r for r in rows)]
    base_cols.append("geodata_name")

    if rows:
        out_df = pd.DataFrame(rows, columns=base_cols)
    else:
        out_df = pd.DataFrame(columns=base_cols)
    out_path = base_dir / "unmapped_geodata.csv"
    out_df.to_csv(out_path, index=False)
    return out_path


def _export_selected_geodata_files(base_dir: Path, selected_source: str) -> None:
    """Optionally copy the selected geodata CSV/GeoJSON into the export folder."""
    mode = get_geodata_export_mode()
    mode = (mode or "no").strip().lower()
    if mode == "no":
        return

    csv_src = Path(selected_source)
    csv_dst = base_dir / csv_src.name

    export_csv = mode in {"csv", "both"}
    export_geojson = mode in {"geojson", "both"}

    if export_csv:
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(csv_src, csv_dst)
            logger.info("Exported geodata CSV to: %s", csv_dst)
        except OSError as exc:
            logger.warning("Could not export geodata CSV %s: %s", csv_src, exc)

    if export_geojson:
        geojson_src: Path | None = None
        try:
            rel = csv_src.resolve().relative_to(GEODATA_CSV_ROOT.resolve())
        except ValueError:
            geojson_src = None
        else:
            geojson_src = (GEOJSON_ROOT / rel).with_suffix(".geojson")

        if geojson_src is None or not geojson_src.is_file():
            logger.warning(
                "No matching GeoJSON file found for geodata CSV %s (expected under %s).",
                csv_src,
                GEOJSON_ROOT,
            )
            return

        try:
            rel_repo_path = geojson_src.resolve().relative_to(PACKAGE_ROOT.parent.resolve())
        except ValueError as exc:
            logger.warning(
                "Could not build GitHub URL for GeoJSON %s: %s",
                geojson_src,
                exc,
            )
            return

        geojson_url = (
            "https://github.com/frievoe97/geo-mapper/raw/refs/heads/main/"
            + rel_repo_path.as_posix()
        )
        txt_dst = base_dir / f"{geojson_src.stem}.txt"
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
            txt_dst.write_text(geojson_url + "\n", encoding="utf-8")
            logger.info("Exported geodata GeoJSON link to: %s", txt_dst)
        except OSError as exc:
            logger.warning("Could not export geodata GeoJSON %s: %s", geojson_src, exc)


def _write_meta_json(
    base_dir: Path,
    parsed_level: str | None,
    parsed_year: str | None,
    dataframe: pd.DataFrame,
) -> Path | None:
    """Write a meta.yaml with column/level/year info based on selections.

    - Start from any existing meta_config (from an input YAML, if provided)
    - Fill/fix fields: id_columns, name_column, value_columns, geodata_level, geodata_year
    - Never modify the original input JSON file; this only writes into the export folder.
    """
    selections = get_selections()
    meta = get_meta_config()
    if not isinstance(meta, dict):
        meta = {}

    # Columns
    # id_columns as a dict: {"<original_index>": "id_col_name", ...}
    if "id_columns" not in meta:
        id_cols = getattr(selections, "id_columns", None) or (
            [selections.id_column] if selections.id_column else []
        )
        id_indices = getattr(selections, "id_column_indices", None) or []
        if id_indices and len(id_indices) == len(id_cols):
            meta["id_columns"] = {
                str(idx): col for idx, col in zip(id_indices, id_cols, strict=False)
            }
        else:
            # Fallback: assign running numbers if no indices are available
            meta["id_columns"] = {str(i): col for i, col in enumerate(id_cols)}
    # name_column as a dict with index and name (if present)
    if "name_column" not in meta:
        if selections.name_column is not None:
            name_index = getattr(selections, "name_column_index", None)
            key = str(name_index) if isinstance(name_index, int) else "0"
            meta["name_column"] = {key: selections.name_column}
        else:
            meta["name_column"] = {}
    # value_columns as a dict: {"<original_index>": "value_col_name", ...}
    if "value_columns" not in meta:
        value_cols = getattr(selections, "value_columns", None) or []
        value_indices = getattr(selections, "value_column_indices", None) or []
        if value_indices and len(value_indices) == len(value_cols):
            meta["value_columns"] = {
                str(idx): col for idx, col in zip(value_indices, value_cols, strict=False)
            }
        else:
            meta["value_columns"] = {str(i): col for i, col in enumerate(value_cols)}

    # Worksheet (Excel sheet name), if present
    if "worksheet" not in meta:
        worksheet = getattr(selections, "worksheet_name", None)
        if worksheet:
            meta["worksheet"] = worksheet

    # Manual mappings: IDs/Names from input and geodata for rows mapped manually
    if "manual_mappings" not in meta:
        manual_entries: list[dict[str, object]] = []
        from .storage import get_geodata_mappings, get_export_geodata_source
        mappings_by_source = get_geodata_mappings()
        export_source = get_export_geodata_source()
        mapping_df = mappings_by_source.get(export_source or "")
        if mapping_df is not None and "mapped_by" in mapping_df.columns:
            try:
                manual_mask = mapping_df["mapped_by"] == "manual"
            except Exception:
                manual_mask = None
            if manual_mask is not None and manual_mask.any():
                id_cols = getattr(selections, "id_columns", None) or (
                    [selections.id_column] if selections.id_column else []
                )
                name_col = (
                    selections.name_column
                    or selections.column
                    or (dataframe.columns[0] if len(dataframe.columns) else None)
                )
                for idx in mapping_df.index[manual_mask]:
                    entry: dict[str, object] = {}
                    # Input IDs as a dict {"0": id_1, "1": id_2, ...}
                    input_ids: dict[str, object] = {}
                    for i, col in enumerate(id_cols):
                        if col in dataframe.columns:
                            val = dataframe.loc[idx, col]
                        else:
                            val = None
                        if val is None or (isinstance(val, float) and pd.isna(val)):
                            input_ids[str(i)] = None
                        else:
                            input_ids[str(i)] = val
                    entry["input_ids"] = input_ids
                    # Input name
                    input_name = None
                    if name_col and name_col in dataframe.columns:
                        val = dataframe.loc[idx, name_col]
                        if not (isinstance(val, float) and pd.isna(val)):
                            input_name = val
                    entry["input_name"] = input_name
                    # Geodata ID/name
                    geodata_id = mapping_df.loc[idx, "mapped_value"] if "mapped_value" in mapping_df.columns else None
                    if geodata_id is None or (isinstance(geodata_id, float) and pd.isna(geodata_id)):
                        entry["geodata_id"] = None
                    else:
                        entry["geodata_id"] = geodata_id
                    geodata_name = mapping_df.loc[idx, "mapped_label"] if "mapped_label" in mapping_df.columns else None
                    if geodata_name is None or (isinstance(geodata_name, float) and pd.isna(geodata_name)):
                        entry["geodata_name"] = None
                    else:
                        entry["geodata_name"] = geodata_name
                    manual_entries.append(entry)
        if manual_entries:
            meta["manual_mappings"] = manual_entries

    # Level (administrative level) -> geodata_level
    if "geodata_level" not in meta:
        level_value: str | None = None
        # Prefer to derive from the export path (it reflects the actual output)
        if parsed_level:
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
        import yaml

        base_dir.mkdir(parents=True, exist_ok=True)
        out_path = base_dir / "meta.yaml"
        with out_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(meta, f, allow_unicode=True, sort_keys=False)
    except OSError as exc:
        logger.warning("Could not write meta.yaml: %s", exc)
        return None

    logger.info("Wrote meta-information to %s.", out_path)
    return out_path


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
    id_cols = getattr(selections, "id_columns", None) or (
        [selections.id_column] if selections.id_column else []
    )
    name_col = (
        selections.name_column
        or selections.column
        or (dataframe.columns[0] if len(dataframe.columns) else None)
    )
    value_cols = [c for c in getattr(selections, "value_columns", []) if c in dataframe.columns]
    input_name = selections.input_name or "input"

    output_root = get_output_root()
    if output_root is None:
        base_root = RESULTS_ROOT / input_name
    else:
        base_root = output_root
    base_dir = base_root

    parsed_level: str | None = None
    parsed_year: str | None = None

    # Target path: [base_root]; derive dataset type/level/year only for metadata.
    src_path = Path(selected_source)
    try:
        version_dir = src_path.parent.name  # e.g. "2013"
        dataset_dir_raw = src_path.parent.parent.name  # e.g. "NUTS_3", "LAU"
    except AttributeError:
        pass
    else:
        parsed_year = version_dir
        raw_upper = dataset_dir_raw.upper()
        if raw_upper == "LAU":
            parsed_level = "LAU"
        # New layout: NUTS_<level>/[year]/file
        m = re.match(r"(NUTS)_(\d+)$", raw_upper)
        if m:
            dataset = m.group(1)
            level = m.group(2)
            parsed_level = f"{dataset} {level}"
        elif raw_upper == "NUTS":
            level_match = re.search(r"_level_(\d+)\.csv$", src_path.name)
            if level_match:
                parsed_level = f"NUTS {level_match.group(1)}"

    mapped_path = _write_mapped_pairs(
        base_dir,
        dataframe,
        selected_sources,
        id_cols,
        name_col,
        value_cols,
    )
    unmapped_input_path = _write_unmapped_original(
        base_dir,
        dataframe,
        selected_sources,
        id_cols,
        name_col,
    )
    unmapped_geodata_path = _write_unmapped_geodata(base_dir, selected_sources)
    _write_meta_json(base_dir, parsed_level, parsed_year, dataframe)
    _export_selected_geodata_files(base_dir, selected_source)

    logger.info("Exported mapped pairs to: %s", mapped_path)
    logger.info("Exported unmapped original values to: %s", unmapped_input_path)
    logger.info("Exported unused geodata rows to: %s", unmapped_geodata_path)

    return dataframe
