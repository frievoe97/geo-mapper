"""Input loading pipeline step (CSV or Excel)."""

from __future__ import annotations

import csv
from pathlib import Path
import logging

import pandas as pd
import questionary

from ..storage import set_input_name


logger = logging.getLogger(__name__)


def _detect_delimiter(path: Path, *, sample_size: int = 4096) -> str:
    """Detect the delimiter of the supplied CSV file."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        sample = handle.read(sample_size)
    if not sample.strip():
        raise ValueError(f"CSV file is empty or contains no detectable data: {path}")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except csv.Error:
        return ","


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"CSV file not found: {path}")
    delimiter = _detect_delimiter(path)
    logger.info("Loading CSV %s (delimiter=%r)", str(path), delimiter)
    return pd.read_csv(path, sep=delimiter)


def _load_excel(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Excel file not found: {path}")
    ext = path.suffix.lower()
    engine = None
    if ext == ".xls":
        engine = "xlrd"
    try:
        xls = pd.ExcelFile(path, engine=engine)
    except Exception as exc:
        raise ValueError(f"Could not open Excel file {path}: {exc}")
    sheets = list(xls.sheet_names)
    if not sheets:
        raise ValueError(f"Excel file has no sheets: {path}")
    default_sheet = sheets[0]
    chosen = questionary.select(
        "Select the worksheet (sheet) to load:",
        choices=sheets,
        default=default_sheet,
    ).ask()
    if not chosen:
        raise SystemExit("No worksheet selected.")
    logger.info("Loading Excel %s sheet=%s", str(path), chosen)
    try:
        return pd.read_excel(path, sheet_name=chosen, engine=engine)
    except Exception as exc:
        raise ValueError(f"Failed reading sheet '{chosen}' from {path}: {exc}")


def _drop_completely_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where all cells are empty (NaN or empty/whitespace strings)."""
    if df.empty:
        return df
    # Start with standard NaN detection
    is_empty = df.isna()
    # Treat empty/whitespace-only strings in object columns as empty as well
    obj_cols = df.select_dtypes(include=["object"]).columns
    if len(obj_cols) > 0:
        # apply element-wise only on object columns, without using deprecated applymap
        stripped = df[obj_cols].apply(
            lambda col: col.map(lambda v: v.strip() if isinstance(v, str) else v)
        )
        is_empty_obj = stripped.eq("") | stripped.isna()
        is_empty.loc[:, obj_cols] = is_empty.loc[:, obj_cols] | is_empty_obj
    cleaned = df.loc[~is_empty.all(axis=1)].copy()
    dropped = len(df) - len(cleaned)
    if dropped > 0:
        logger.info("Dropped %d completely empty input row(s).", dropped)
    return cleaned


def load_csv_step(path: Path) -> pd.DataFrame:
    """Load CSV or Excel into a DataFrame as the first pipeline step."""
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")
    # Remember the input file name (without extension) for result paths
    set_input_name(path.stem)
    ext = path.suffix.lower()
    if ext == ".csv":
        df = _load_csv(path)
    elif ext in {".xlsx", ".xlsm", ".xls"}:
        df = _load_excel(path)
    else:
        raise ValueError(f"Unsupported file extension for input: {ext} (supported: .csv, .xlsx, .xlsm)")

    # Remove rows that are completely empty after load
    df = _drop_completely_empty_rows(df)
    return df
