"""Exact-ID mapper: map rows where the ID matches exactly any geodata ID column.

Rules:
- IDs are compared as plain strings (optionally stripping leading zeros).
- Input data can provide multiple ID columns; geodata can publish multiple ID columns.
- Any column whose header starts with ``id`` (case-insensitive) is considered.
- A mapping is only produced when every matching input ID resolves to the same geodata row.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Tuple
import numbers

import pandas as pd

logger = logging.getLogger(__name__)


def _normalize_id(value: object, strip_leading_zeroes: bool) -> str | None:
    """Convert a cell value to a string while keeping NaN/None as None."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        # Some objects (e.g. lists) do not support isna checks – fall through.
        pass
    text: str
    if isinstance(value, numbers.Integral):
        text = str(value)
    elif isinstance(value, numbers.Real):
        if float(value).is_integer():
            text = str(int(value))
        else:
            text = str(value)
    else:
        text = str(value)
    if not text:
        return None
    if strip_leading_zeroes:
        stripped = text.lstrip("0")
        text = stripped if stripped else "0"
    return text


def _id_columns(frame: pd.DataFrame) -> list[str]:
    """Return all column names that look like ID columns (prefix ``id``)."""
    columns: list[str] = []
    for col in frame.columns:
        if not isinstance(col, str):
            continue
        if not col.lower().startswith("id"):
            continue
        columns.append(col)
    return columns


def _build_lookup(
    frame: pd.DataFrame, id_cols: list[str], *, strip_leading_zeroes: bool
) -> Dict[str, List[Tuple[str, str, str]]]:
    """Create a lookup from ID value to canonical geodata id/label/matched column."""
    lookup: Dict[str, List[Tuple[str, str, str]]] = {}
    if "id" not in frame.columns:
        return lookup

    names = frame["name"] if "name" in frame.columns else pd.Series(pd.NA, index=frame.index)

    for idx, canonical in frame["id"].items():
        canon_str = _normalize_id(canonical, strip_leading_zeroes)
        if canon_str is None:
            continue
        label = names.at[idx]
        label_str = "" if pd.isna(label) else str(label)
        for col in id_cols:
            val = frame.at[idx, col]
            value_str = _normalize_id(val, strip_leading_zeroes)
            if value_str is None:
                continue
            lookup.setdefault(value_str, []).append((canon_str, label_str, col))
    return lookup


def _empty_output(index: pd.Index) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "mapped_by": pd.Series(pd.NA, index=index, dtype="object"),
            "mapped_value": pd.Series(pd.NA, index=index, dtype="object"),
            "mapped_source": pd.Series(pd.NA, index=index, dtype="object"),
            "mapped_label": pd.Series(pd.NA, index=index, dtype="object"),
            "mapped_param": pd.Series(pd.NA, index=index, dtype="object"),
        }
    )


def _map_single_frame(
    df_slice: pd.DataFrame,
    csv_path: Path,
    frame: pd.DataFrame,
    source_cols: list[str],
    *,
    strip_leading_zeroes: bool,
) -> pd.DataFrame:
    """Map a single geodata frame and return a DataFrame aligned to df_slice."""
    out = _empty_output(df_slice.index)
    if "id" not in frame.columns:
        return out

    valid_source_cols = [col for col in source_cols if col in df_slice.columns]
    if not valid_source_cols:
        return out

    id_cols = _id_columns(frame)
    if not id_cols:
        return out

    lookup = _build_lookup(frame, id_cols, strip_leading_zeroes=strip_leading_zeroes)
    if not lookup:
        return out

    normalized_inputs: dict[str, pd.Series] = {}
    for col in valid_source_cols:
        normalized_inputs[col] = df_slice[col].map(
            lambda v: _normalize_id(v, strip_leading_zeroes)
        )

    mapped_count = 0
    mapper_label = "id_without_leading_zero" if strip_leading_zeroes else "exact_id"

    for row_idx in df_slice.index:
        row_hits: list[Tuple[str, str, str, str]] = []
        for input_col in valid_source_cols:
            key = normalized_inputs[input_col].at[row_idx]
            if key is None:
                continue
            hits = lookup.get(key)
            if not hits:
                continue
            unique_ids = {gid for gid, _label, _col in hits}
            if len(unique_ids) != 1:
                continue
            gid = next(iter(unique_ids))
            label = ""
            matched_column = ""
            for cand_gid, cand_label, cand_col in hits:
                if cand_gid == gid:
                    label = cand_label
                    matched_column = cand_col
                    break
            row_hits.append((gid, label, matched_column, input_col))
        if not row_hits:
            continue
        unique_candidate_ids = {gid for gid, _label, _geo_col, _input in row_hits}
        if len(unique_candidate_ids) != 1:
            continue
        gid, label, matched_column, input_col = row_hits[0]
        out.at[row_idx, "mapped_by"] = mapper_label
        out.at[row_idx, "mapped_value"] = gid
        out.at[row_idx, "mapped_source"] = str(csv_path)
        out.at[row_idx, "mapped_label"] = label
        if matched_column:
            out.at[row_idx, "mapped_param"] = f"{matched_column}"
        else:
            out.at[row_idx, "mapped_param"] = input_col or pd.NA
        mapped_count += 1

    logger.debug(
        "%s result: %d/%d mapped for %s using ID column(s) %s (geodata columns: %s)",
        mapper_label,
        mapped_count,
        len(out),
        Path(csv_path).name,
        ", ".join(valid_source_cols),
        ", ".join(id_cols),
    )
    return out


def exact_id_mapper(
    df_slice: pd.DataFrame,
    geodata_frames: List[Tuple[Path, pd.DataFrame]],
    source_cols: list[str],
) -> pd.DataFrame:
    """Map IDs for the provided geodata frame (expects exactly one frame)."""
    if not geodata_frames:
        return _empty_output(df_slice.index)
    csv_path, frame = geodata_frames[0]
    return _map_single_frame(
        df_slice,
        csv_path,
        frame,
        source_cols,
        strip_leading_zeroes=False,
    )


def id_without_leading_zero_mapper(
    df_slice: pd.DataFrame,
    geodata_frames: List[Tuple[Path, pd.DataFrame]],
    source_cols: list[str],
) -> pd.DataFrame:
    """Map IDs after removing leading zeros."""
    if not geodata_frames:
        return _empty_output(df_slice.index)
    csv_path, frame = geodata_frames[0]
    return _map_single_frame(
        df_slice,
        csv_path,
        frame,
        source_cols,
        strip_leading_zeroes=True,
    )
