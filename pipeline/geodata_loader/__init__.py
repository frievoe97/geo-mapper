"""Pipeline step that loads matching geodata CSV files."""

from __future__ import annotations

from pathlib import Path
import logging
from typing import List, Optional

import pandas as pd

from ..storage import get_selections, set_geodata_frames

from ..geodata_selection import DATASET_CHOICES, UNKNOWN_OPTION

GEODATA_CSV_ROOT = Path("geodata_clean") / "csv"

logger = logging.getLogger(__name__)


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
        logger.info("  - %s", str(path))


def load_geodata_files_step(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Load the matching geodata CSV files into memory."""
    paths = _collect_matching_csv_paths()
    _report_loading(paths)
    frames: list[tuple[Path, pd.DataFrame]] = []
    for csv_path in paths:
        try:
            frames.append((csv_path, pd.read_csv(csv_path)))
        except pd.errors.EmptyDataError:
            continue
    set_geodata_frames(frames)
    return dataframe
