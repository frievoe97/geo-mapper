"""Central storage for the interactive selections made by the pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd


@dataclass
class PipelineSelections:
    """Record the choices made during the pipeline run."""

    input_name: Optional[str] = None
    # Backwards-compatible single source column (historically the name column)
    column: Optional[str] = None
    # Optional dedicated selections for ID and name columns
    id_column: Optional[str] = None
    id_columns: List[str] = field(default_factory=list)
    id_column_indices: List[int] = field(default_factory=list)
    name_column: Optional[str] = None
    name_column_index: Optional[int] = None
    # Optional additional value columns to carry through and export
    value_columns: List[str] = field(default_factory=list)
    value_column_indices: List[int] = field(default_factory=list)
    geodata_type: Optional[str] = None
    nuts_level: Optional[str] = None
    geodata_version: Optional[str] = None
    # Optional: selected worksheet name for Excel inputs
    worksheet_name: Optional[str] = None
    # json_path: Optional[Path] = None
    # Optional meta-configuration loaded from a JSON file (e.g. column and geodata hints)
    meta_config: Optional[Dict[str, Any]] = None
    # Optional: decide automatically which mappers to run (skip interactive UI)
    auto_select_mappers: bool = False
    # Optional: automatically use the first geodata source for export/manual mapping
    auto_export_source: bool = False
    geodata_frames: List[Tuple[Path, "pd.DataFrame"]] = field(default_factory=list)
    selected_mappers: Optional[List[str]] = None
    geodata_usage: Dict[str, int] = field(default_factory=dict)
    geodata_total_rows: Optional[int] = None
    geodata_used_ids_by_source: Dict[str, int] = field(default_factory=dict)
    geodata_rows_by_source: Dict[str, int] = field(default_factory=dict)
    geodata_mappings_by_source: Dict[str, "pd.DataFrame"] = field(default_factory=dict)
    export_geodata_source: Optional[str] = None
    output_root: Optional[Path] = None
    geodata_export_mode: Optional[str] = None


SELECTIONS = PipelineSelections()


def set_column(column: str) -> None:
    """Set the primary source column (for backwards compatibility) and treat it as name column."""
    SELECTIONS.column = column
    SELECTIONS.name_column = column


def set_id_columns(columns: List[str]) -> None:
    """Set the selected ID columns."""
    unique = []
    for col in columns:
        if col is None:
            continue
        if col not in unique:
            unique.append(col)
    SELECTIONS.id_columns = unique
    SELECTIONS.id_column = unique[0] if unique else None


# def get_id_columns() -> List[str]:
#     """Return the list of selected ID columns (may be empty)."""
#     return list(SELECTIONS.id_columns)


# def set_id_column(column: Optional[str]) -> None:
#     """Backwards-compatible setter for a single ID column."""
#     if column is None:
#         set_id_columns([])
#     else:
#         set_id_columns([column])


def set_name_column(column: Optional[str]) -> None:
    """Set the selected name column and keep the legacy 'column' in sync."""
    SELECTIONS.name_column = column
    if column is not None:
        SELECTIONS.column = column


def set_value_columns(columns: List[str]) -> None:
    """Set the selected value columns (export-only)."""
    SELECTIONS.value_columns = list(columns)


def set_id_column_indices(indices: List[int]) -> None:
    """Store positional indices of the selected ID columns in the original input."""

    SELECTIONS.id_column_indices = list(indices)


def set_name_column_index(index: Optional[int]) -> None:
    """Store positional index of the selected name column in the original input."""

    SELECTIONS.name_column_index = index


def set_value_column_indices(indices: List[int]) -> None:
    """Store positional indices of the selected value columns in the original input."""

    SELECTIONS.value_column_indices = list(indices)


def set_input_name(name: str) -> None:
    SELECTIONS.input_name = name


def set_output_root(path: Optional[Path]) -> None:
    """Remember the base directory where exports should be written."""

    SELECTIONS.output_root = path


def get_output_root() -> Optional[Path]:
    """Return the base directory for exports, if configured."""

    return SELECTIONS.output_root


def set_worksheet_name(name: Optional[str]) -> None:
    """Remember the selected worksheet name for Excel inputs."""

    SELECTIONS.worksheet_name = name


def get_worksheet_name() -> Optional[str]:
    """Return the selected worksheet name, if any."""

    return SELECTIONS.worksheet_name


def set_geodata_export_mode(mode: Optional[str]) -> None:
    """Configure whether the selected geodata should be exported (csv/geojson/both)."""

    if mode is None:
        SELECTIONS.geodata_export_mode = None
        return
    normalized = str(mode).strip().lower()
    if normalized not in {"no", "csv", "geojson", "both"}:
        normalized = "no"
    SELECTIONS.geodata_export_mode = normalized


def get_geodata_export_mode() -> str:
    """Return the configured geodata export mode (no|csv|geojson|both)."""

    mode = SELECTIONS.geodata_export_mode
    if not mode:
        return "no"
    return str(mode).strip().lower()


def set_json_path(path: Optional[Path]) -> None:
    """Store an optional JSON path provided on the CLI."""

    SELECTIONS.json_path = path


def set_meta_config(config: Optional[Dict[str, Any]]) -> None:
    """Store optional meta configuration loaded from JSON."""

    SELECTIONS.meta_config = dict(config) if config is not None else None


def set_auto_select_mappers(flag: bool) -> None:
    """Enable or disable automatic mapper selection (skip UI)."""

    SELECTIONS.auto_select_mappers = bool(flag)


def set_export_geodata_source(source: Optional[str]) -> None:
    """Store the single geodata CSV path chosen for export/manual mapping."""

    SELECTIONS.export_geodata_source = source


def get_export_geodata_source() -> Optional[str]:
    """Return the geodata CSV path chosen for export/manual mapping."""

    return SELECTIONS.export_geodata_source


# def get_json_path() -> Optional[Path]:
#     """Return the optional JSON path provided on the CLI."""
#
#     return SELECTIONS.json_path


def get_meta_config() -> Optional[Dict[str, Any]]:
    """Return the optional meta configuration loaded from JSON."""

    return SELECTIONS.meta_config


def get_auto_select_mappers() -> bool:
    """Return whether mapper selection should be skipped and defaults used."""

    return bool(SELECTIONS.auto_select_mappers)


def set_auto_export_source(flag: bool) -> None:
    """Enable or disable automatic selection of first geodata source for export/manual mapping."""

    SELECTIONS.auto_export_source = bool(flag)


def get_auto_export_source() -> bool:
    """Return whether first geodata source should be chosen automatically."""

    return bool(SELECTIONS.auto_export_source)


def set_geodata_type(value: str) -> None:
    SELECTIONS.geodata_type = value


def set_nuts_level(value: str) -> None:
    SELECTIONS.nuts_level = value


def set_geodata_version(value: str) -> None:
    SELECTIONS.geodata_version = value


def set_geodata_frames(frames: List[Tuple[Path, pd.DataFrame]]) -> None:
    SELECTIONS.geodata_frames = frames


def get_geodata_frames() -> List[Tuple[Path, pd.DataFrame]]:
    return SELECTIONS.geodata_frames


def set_selected_mappers(mappers: List[str]) -> None:
    SELECTIONS.selected_mappers = mappers


def get_selected_mappers() -> Optional[List[str]]:
    return SELECTIONS.selected_mappers


def get_selections() -> PipelineSelections:
    """Return the current stored selections."""

    return SELECTIONS


def set_geodata_usage(
    usage: Dict[str, int],
    total_rows: int,
    used_ids_by_source: Optional[Dict[str, int]] = None,
    geodata_rows_by_source: Optional[Dict[str, int]] = None,
) -> None:
    """Store per-geodata usage counts and total number of input/geodata rows.

    usage:                 number of input rows mapped per geodata CSV
    total_rows:            total number of input rows
    used_ids_by_source:    number of distinct geodata IDs used per CSV
    geodata_rows_by_source:number of geodata rows (with IDs) per CSV
    """

    SELECTIONS.geodata_usage = dict(usage)
    SELECTIONS.geodata_total_rows = int(total_rows)
    if used_ids_by_source is not None:
        SELECTIONS.geodata_used_ids_by_source = dict(used_ids_by_source)
    if geodata_rows_by_source is not None:
        SELECTIONS.geodata_rows_by_source = dict(geodata_rows_by_source)


def get_geodata_usage() -> Tuple[Dict[str, int], Optional[int]]:
    """Return stored per-geodata usage counts and total rows, if available."""

    return SELECTIONS.geodata_usage, SELECTIONS.geodata_total_rows


def get_geodata_geocoverage() -> Tuple[Dict[str, int], Dict[str, int]]:
    """Return per-geodata coverage: used IDs and total geodata rows per source."""

    return SELECTIONS.geodata_used_ids_by_source, SELECTIONS.geodata_rows_by_source


def set_geodata_mappings(mappings: Dict[str, "pd.DataFrame"]) -> None:
    """Store per-geodata CSV mapping DataFrames (one mapping per row per CSV)."""

    SELECTIONS.geodata_mappings_by_source = dict(mappings)


def get_geodata_mappings() -> Dict[str, "pd.DataFrame"]:
    """Return stored per-geodata CSV mapping DataFrames."""

    return SELECTIONS.geodata_mappings_by_source
