"""Mapper: append suffixes, normalize and sort tokens.

Workflow (per source value):
- for each suffix in ``SUFFIX_TITLE_WORDS``:
  - build variant: ``<Input> + " " + <Suffix>``
  - normalize the string
  - split on whitespace into tokens, sort tokens alphabetically
    and join them back into a key
- for all geodata names, build the same normalized, token-sorted key
- mapping occurs when across all variants exactly ONE unique match
  (a geodata ID) exists
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple, cast

import pandas as pd

from ...constants import SUFFIX_TITLE_WORDS
from ...utils.text import normalize_string


def _token_key(text: str) -> str:
    """Normalize text, sort tokens alphabetically and build a key."""

    norm = normalize_string(text)
    if not norm:
        return ""
    tokens = norm.split()
    tokens.sort()
    return " ".join(tokens)


def _build_geodata_lookup(frame: pd.DataFrame) -> Dict[str, List[Tuple[str, str]]]:
    """Build lookup: normalized, token-sorted name -> list of (id, original_name)."""

    lookup: Dict[str, List[Tuple[str, str]]] = {}
    for raw, gid in zip(frame.get("name", []), frame.get("id", []), strict=False):
        key = _token_key(str(raw))
        if not key:
            continue
        lookup.setdefault(key, []).append((str(gid), str(raw)))
    return lookup


# Optional: previously used geodata IDs per CSV, set by the orchestrator.
USED_IDS_BY_SOURCE: Dict[str, set[str]] = {}


def set_used_ids_for_source(source: str, ids: set) -> None:
    """Record already-used geodata IDs for a given CSV."""

    USED_IDS_BY_SOURCE[source] = {str(gid) for gid in ids}


def token_permutation_mapper(
    df_slice: pd.DataFrame, geodata_frames: List[Tuple[Path, pd.DataFrame]], source_col: str
) -> pd.DataFrame:
    """Mapper that appends suffixes, normalizes and sorts tokens.

    For each input value:
    - for each suffix in ``SUFFIX_TITLE_WORDS`` a variant ``"<Input> " + Suffix`` is created
    - each variant is normalized, split into tokens, tokens are sorted
      alphabetically and joined back into a key
    - for the geodata the same key is built per name
    - if across all variants exactly one geodata ID is found
      (taking already-used IDs into account), a mapping is created
    """

    if not geodata_frames:
        return pd.DataFrame(
            index=df_slice.index,
            columns=["mapped_by", "mapped_value", "mapped_source", "mapped_label", "mapped_param"],
        ).assign(
            mapped_by=cast(Any, pd.NA),
            mapped_value=cast(Any, pd.NA),
            mapped_source=cast(Any, pd.NA),
            mapped_label=cast(Any, pd.NA),
            mapped_param=cast(Any, pd.NA),
        )

    csv_path, frame = geodata_frames[0]
    if not {"name", "id"}.issubset(frame.columns):
        return pd.DataFrame(
            index=df_slice.index,
            columns=["mapped_by", "mapped_value", "mapped_source", "mapped_label", "mapped_param"],
        ).assign(
            mapped_by=cast(Any, pd.NA),
            mapped_value=cast(Any, pd.NA),
            mapped_source=cast(Any, pd.NA),
            mapped_label=cast(Any, pd.NA),
            mapped_param=cast(Any, pd.NA),
        )

    lookup = _build_geodata_lookup(frame)
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

        # Variants: original itself + original + " " + suffix (for each suffix)
        variants: List[str] = [original]
        for suffix in SUFFIX_TITLE_WORDS:
            variants.append(f"{original} {suffix}")

        # For each variant, build the normalized, token-sorted key
        # and collect unique candidates (after filtering already-used IDs).
        # We keep the key itself so it can later be stored in mapped_param.
        hits: List[Tuple[str, str, str]] = []  # (normalized_key, geodata_id, geodata_label)
        for variant in variants:
            key = _token_key(variant)
            if not key:
                continue
            candidates = lookup.get(key, [])
            # IDs that were already used in earlier steps are ignored so that,
            # for example, after mapping the kreisfreie Stadt the Landkreis
            # variant can still be uniquely assigned.
            available = [(gid, label) for gid, label in candidates if str(gid) not in used_ids]
            if len(available) == 1:
                gid, label = available[0]
                hits.append((key, gid, label))

        # Only unique if all hits point to the same ID.
        if hits:
            unique_ids = {gid for _key, gid, _label in hits}
            if len(unique_ids) == 1:
                used_key, hit_id, hit_label = hits[0]
            else:
                used_key = hit_id = hit_label = None
        else:
            used_key = hit_id = hit_label = None

        if hit_id is not None:
            out_rows["mapped_by"].append("token_permutation")
            out_rows["mapped_value"].append(hit_id)
            out_rows["mapped_source"].append(str(csv_path))
            out_rows["mapped_label"].append(hit_label)
            # mapped_param: which normalized token key matched
            out_rows["mapped_param"].append(used_key)
        else:
            out_rows["mapped_by"].append(pd.NA)
            out_rows["mapped_value"].append(pd.NA)
            out_rows["mapped_source"].append(pd.NA)
            out_rows["mapped_label"].append(pd.NA)
            out_rows["mapped_param"].append(pd.NA)

    return pd.DataFrame(out_rows, index=df_slice.index)
