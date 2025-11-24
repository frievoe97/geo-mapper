"""CLI entry point for simple geo mapper helpers."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from geo_mapper.pipeline import (
    load_data_step,
    load_geodata_files_step,
    narrow_to_single_column_step,
    normalize_source_step,
    select_geodata_step,
    select_mappers_step,
    mapping_step,
    select_export_geodata_step,
    manual_mapping_step,
    export_results_step,
)
from geo_mapper.pipeline.storage import (
    set_json_path,
    set_meta_config,
)

PIPELINE_STEPS = (
    load_data_step,
    narrow_to_single_column_step,
    normalize_source_step,
    # Default to loading all candidate datasets
    select_geodata_step,
    load_geodata_files_step,
    select_mappers_step,
    mapping_step,
    select_export_geodata_step,
    manual_mapping_step,
    export_results_step,
)


def _parse_bool_flag(value: str) -> bool:
    """Parse common truthy string representations into a boolean."""
    return str(value).lower() in ("1", "true", "yes", "y")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Load a single CSV/Excel into memory.")
    parser.add_argument(
        "--data",
        "-d",
        dest="data_file",
        type=Path,
        required=True,
        help="Path to a CSV (.csv) or Excel (.xlsx/.xlsm) file to load.",
    )
    parser.add_argument(
        "--json",
        "-j",
        dest="json_file",
        type=Path,
        required=False,
        help="Optional path to a JSON file that will be copied into the export folder.",
    )
    parser.add_argument(
        "--auto-mappers",
        dest="auto_mappers",
        type=_parse_bool_flag,
        default=True,
        help="If true (default), skip interactive mapper selection and use default mappers.",
    )
    parser.add_argument(
        "--auto-export-source",
        dest="auto_export_source",
        type=_parse_bool_flag,
        default=False,
        help="If true, automatically use the first geodata source for export/manual mapping instead of asking.",
    )
    parser.add_argument(
        "--export-geodata",
        dest="export_geodata",
        choices=("no", "csv", "geojson", "both"),
        default="no",
        help=(
            "Control whether the selected geodata dataset is also written into the results "
            "folder: 'no' (default), 'csv', 'geojson', or 'both'."
        ),
    )
    return parser.parse_args()


def run_pipeline(data_file: Path) -> pd.DataFrame:
    """Execute the defined pipeline steps."""
    current: Any = data_file
    for step in PIPELINE_STEPS:
        current = step(current)
    return current


def main() -> None:
    """Parse arguments and delegate to the pipeline runner."""
    # Basic logging setup so users see progress and warnings on the console
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args()
    # Store optional JSON metadata path in central storage so export/meta helpers can use it.
    # If provided, try to load a meta configuration dictionary from the JSON file.
    set_json_path(args.json_file)
    if args.json_file is not None:
        try:
            with args.json_file.open("r", encoding="utf-8") as f:
                meta = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            logging.warning(
                "JSON metadata file %s could not be read: %s",
                args.json_file,
                exc,
            )
            set_meta_config(None)
        else:
            if isinstance(meta, dict):
                set_meta_config(meta)
            else:
                logging.warning(
                    "JSON metadata file %s does not contain a JSON object; ignoring it.",
                    args.json_file,
                )
                set_meta_config(None)
    else:
        set_meta_config(None)

    # Configure whether mapper selection runs interactively or uses defaults,
    # and whether the export geodata source is chosen automatically.
    from geo_mapper.pipeline.storage import (
        set_auto_select_mappers,
        set_auto_export_source,
        set_geodata_export_mode,
    )  # import locally to avoid cycles

    set_auto_select_mappers(args.auto_mappers)
    set_auto_export_source(args.auto_export_source)
    set_geodata_export_mode(args.export_geodata)
    try:
        dataframe = run_pipeline(args.data_file)
    except (FileNotFoundError, ValueError, pd.errors.EmptyDataError, SystemExit) as exc:
        logging.error(str(exc))
        sys.exit(1)

    # keep the reference so downstream tooling can import `main` and reuse the dataframe
    globals()["dataframe"] = dataframe


if __name__ == "__main__":
    main()
