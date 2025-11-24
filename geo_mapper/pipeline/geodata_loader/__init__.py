"""Pipeline step that loads matching geodata CSV files."""

from __future__ import annotations

from pathlib import Path
import logging
from typing import Optional

import pandas as pd

from ..storage import get_selections, set_geodata_frames
from ..constants import (
    GEODATA_CSV_ROOT,
    DATASET_CHOICES,
    UNKNOWN_OPTION,
    infer_dataset_family,
)

logger = logging.getLogger(__name__)


def _ensure_standard_id_column(csv_path: Path, frame: pd.DataFrame) -> pd.DataFrame:
    """Add a canonical 'id' column for datasets that renamed their identifier fields."""

    if "id" in frame.columns:
        return frame

    dataset = infer_dataset_family(csv_path)
    if dataset == "nuts":
        id_source = None
        if "id_nuts" in frame.columns:
            id_source = "id_nuts"
        elif "id_ars" in frame.columns:
            id_source = "id_ars"

        if id_source is not None:
            frame = frame.copy()
            if id_source == "id_nuts" and "id_ars" in frame.columns:
                frame["id"] = frame["id_nuts"].fillna(frame["id_ars"])
            else:
                frame["id"] = frame[id_source]
            logger.debug(
                "Normalized geodata IDs for %s using column '%s'",
                csv_path.name,
                id_source,
            )
    return frame


def _dataset_types_to_load(geodata_type: Optional[str]) -> list[str]:
    if not geodata_type or geodata_type == UNKNOWN_OPTION:
        return [choice.lower() for choice in DATASET_CHOICES]
    return [geodata_type.lower()]


def _version_dirs_for_dataset(dataset_dir: Path, version_selection: Optional[str]) -> list[Path]:
    if version_selection and version_selection != UNKNOWN_OPTION:
        candidate = dataset_dir / version_selection
        return [candidate] if candidate.is_dir() else []
    return sorted(
        (entry for entry in dataset_dir.iterdir() if entry.is_dir()), key=lambda p: p.name
    )


def _collect_matching_csv_paths() -> list[Path]:
    selections = get_selections()
    paths: list[Path] = []
    for dataset in _dataset_types_to_load(selections.geodata_type):
        base_dirs: list[Path] = []
        if dataset == "lau":
            candidate = GEODATA_CSV_ROOT / "LAU"
            if candidate.exists():
                base_dirs.append(candidate)
        elif dataset == "nuts":
            # New layout: NUTS_<level>/[year]/file
            if selections.nuts_level not in (None, UNKNOWN_OPTION):
                candidate = GEODATA_CSV_ROOT / f"NUTS_{selections.nuts_level}"
                if candidate.exists():
                    base_dirs.append(candidate)
            else:
                # If no level was chosen, consider all NUTS_* directories
                if GEODATA_CSV_ROOT.exists():
                    for entry in GEODATA_CSV_ROOT.iterdir():
                        if entry.is_dir() and entry.name.upper().startswith("NUTS_"):
                            base_dirs.append(entry)

        for dataset_dir in base_dirs:
            for version_dir in _version_dirs_for_dataset(dataset_dir, selections.geodata_version):
                for csv_file in sorted(version_dir.glob("*.csv")):
                    if dataset == "nuts" and selections.nuts_level not in (
                        None,
                        UNKNOWN_OPTION,
                    ):
                        if f"level_{selections.nuts_level}" not in csv_file.name:
                            continue
                    paths.append(csv_file)
    return paths


def _report_loading(paths: list[Path]) -> None:
    if not paths:
        logger.info("No geodata CSV files matched the selection.")
        return
    logger.info("Loading geodata CSV files:")

    for path in paths:
        path_display = path.name  # nur Dateiname
        logger.info("%s", path_display)


def load_geodata_files_step(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Load the matching geodata CSV files into memory."""
    paths = _collect_matching_csv_paths()
    _report_loading(paths)
    frames: list[tuple[Path, pd.DataFrame]] = []
    for csv_path in paths:
        try:
            frame = pd.read_csv(csv_path, dtype=str)
            frame = _ensure_standard_id_column(csv_path, frame)
            frames.append((csv_path, frame))
        except pd.errors.EmptyDataError:
            continue
    set_geodata_frames(frames)
    return dataframe
