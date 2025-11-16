"""Mapper: apply regex replacements to create variants, then normalized exact match.

Applies a list of (pattern, replacement) rules to create string variants; any
variant that normalizes to a unique geodata name triggers a mapping.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Tuple, cast

import pandas as pd

from ...utils.text import normalize_string
from ...constants import REGEX_REPLACEMENTS


REPLACEMENTS: List[Tuple[str, str]] = list(REGEX_REPLACEMENTS)

# Optional: already-used geodata IDs per CSV, set by the orchestrator.
USED_IDS_BY_SOURCE: Dict[str, set[str]] = {}


def set_used_ids_for_source(source: str, ids: set) -> None:
    """Record already-used geodata IDs for a given CSV path."""

    USED_IDS_BY_SOURCE[source] = {str(gid) for gid in ids}


def _apply_replacements(text: str) -> List[str]:
    s = str(text)
    variants = {s}
    for pat, repl in REPLACEMENTS:
        new_variants = set()
        for v in variants:
            new_variants.add(re.sub(pat, repl, v, flags=re.IGNORECASE))
        variants |= new_variants
    return list(variants)


def _build_norm_lookup(frame: pd.DataFrame) -> Dict[str, List[Tuple[str, str]]]:
    lookup: Dict[str, List[Tuple[str, str]]] = {}
    for raw, gid in zip(frame.get("name", []), frame.get("id", []), strict=False):
        norm = normalize_string(raw)
        if not norm:
            continue
        lookup.setdefault(norm, []).append((str(gid), str(raw)))
    return lookup


def regex_replace_mapper(
    df_slice: pd.DataFrame, geodata_frames: List[Tuple[Path, pd.DataFrame]], source_col: str
) -> pd.DataFrame:
    if not geodata_frames:
        return pd.DataFrame(index=df_slice.index, columns=["mapped_by", "mapped_value", "mapped_source", "mapped_label", "mapped_param"]).assign(
            mapped_by=cast(Any, pd.NA),
            mapped_value=cast(Any, pd.NA),
            mapped_source=cast(Any, pd.NA),
            mapped_label=cast(Any, pd.NA),
            mapped_param=cast(Any, pd.NA),
        )
    csv_path, frame = geodata_frames[0]
    if not {"name", "id"}.issubset(frame.columns):
        return pd.DataFrame(index=df_slice.index, columns=["mapped_by", "mapped_value", "mapped_source", "mapped_label", "mapped_param"]).assign(
            mapped_by=cast(Any, pd.NA),
            mapped_value=cast(Any, pd.NA),
            mapped_source=cast(Any, pd.NA),
            mapped_label=cast(Any, pd.NA),
            mapped_param=cast(Any, pd.NA),
        )

    lookup = _build_norm_lookup(frame)
    used_ids = USED_IDS_BY_SOURCE.get(str(csv_path), set())
    out_rows = {
        "mapped_by": [],
        "mapped_value": [],
        "mapped_source": [],
        "mapped_label": [],
        "mapped_param": [],
    }
    for i in df_slice.index:
        original = str(df_slice.at[i, source_col])
        variants = _apply_replacements(original)
        # Collect all unique candidates after filtering out already-used IDs.
        # We only map if all unique hits point to the same geodata ID.
        hits: List[Tuple[str, str, str]] = []  # (gid, label, variant)
        for var in variants:
            norm = normalize_string(var)
            cands = lookup.get(norm, [])
            # Filter out IDs already used by earlier mappers so a remaining
            # unique candidate can still be chosen here.
            available = [(gid, label) for gid, label in cands if str(gid) not in used_ids]
            if len(available) == 1:
                gid, label = available[0]
                hits.append((gid, label, var))

        # Only map if all unique matches point to the same ID.
        if hits:
            unique_ids = {gid for gid, _label, _var in hits}
            if len(unique_ids) == 1:
                hit_id, hit_label, used_rule = hits[0]
            else:
                hit_id = hit_label = used_rule = None
        else:
            hit_id = hit_label = used_rule = None

        if hit_id is not None:
            out_rows["mapped_by"].append("regex_replace")
            out_rows["mapped_value"].append(hit_id)
            out_rows["mapped_source"].append(str(csv_path))
            out_rows["mapped_label"].append(hit_label)
            # parameter: normalized variant that led to the match
            out_rows["mapped_param"].append(normalize_string(used_rule) if used_rule is not None else pd.NA)
        else:
            out_rows["mapped_by"].append(pd.NA)
            out_rows["mapped_value"].append(pd.NA)
            out_rows["mapped_source"].append(pd.NA)
            out_rows["mapped_label"].append(pd.NA)
            out_rows["mapped_param"].append(pd.NA)
    return pd.DataFrame(out_rows, index=df_slice.index)
