"""Mapper: token permutation normalized exact match (join without spaces).

Generates all permutations of tokens (bounded) for each source value, joins
without spaces, normalizes, and checks for a unique match against geodata
names that are likewise normalized and stripped of spaces.
"""

from __future__ import annotations

import itertools
import re
from pathlib import Path
from typing import Dict, List, Tuple, cast, Any

import pandas as pd

from ...utils.text import normalize_string


PUNCT_RE = re.compile(r"[^0-9a-zA-ZäöüÄÖÜß]+")


def _tokens_from_text(s: str) -> List[str]:
    s = str(s)
    s = PUNCT_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s.split(" ") if s else []


def _build_lookup_no_space(frame: pd.DataFrame) -> Dict[str, List[Tuple[str, str]]]:
    lookup: Dict[str, List[Tuple[str, str]]] = {}
    for raw, gid in zip(frame.get("name", []), frame.get("id", []), strict=False):
        norm = normalize_string(raw).replace(" ", "")
        if not norm:
            continue
        lookup.setdefault(norm, []).append((str(gid), str(raw)))
    return lookup


def token_permutation_mapper(
    df_slice: pd.DataFrame, geodata_frames: List[Tuple[Path, pd.DataFrame]], source_col: str, MAX_TOKENS: int = 6
) -> pd.DataFrame:
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

    lookup = _build_lookup_no_space(frame)
    out_rows = {
        "mapped_by": [],
        "mapped_value": [],
        "mapped_source": [],
        "mapped_label": [],
        "mapped_param": [],
    }
    for i in df_slice.index:
        original = str(df_slice.at[i, source_col])
        toks = _tokens_from_text(original)
        if len(toks) == 0 or len(toks) > MAX_TOKENS:
            out_rows["mapped_by"].append(pd.NA)
            out_rows["mapped_value"].append(pd.NA)
            out_rows["mapped_source"].append(pd.NA)
            out_rows["mapped_label"].append(pd.NA)
            out_rows["mapped_param"].append(pd.NA)
            continue
        variants_norm = set()
        variant_map = {}
        for perm in set(itertools.permutations(toks, len(toks))):
            joined = "".join(perm)
            norm_joined = normalize_string(joined).replace(" ", "")
            variants_norm.add(norm_joined)
            # Remember the original permutation string for debugging/parameters
            variant_map.setdefault(norm_joined, joined)
        hit_id = hit_label = None
        used_perm = None
        for key in variants_norm:
            cands = lookup.get(key, [])
            if len(cands) == 1:
                hit_id, hit_label = cands[0]
                used_perm = variant_map.get(key, key)
                break
        if hit_id is not None:
            out_rows["mapped_by"].append("token_permutation")
            out_rows["mapped_value"].append(hit_id)
            out_rows["mapped_source"].append(str(csv_path))
            out_rows["mapped_label"].append(hit_label)
            # parameter: which token join/permutation matched
            out_rows["mapped_param"].append(used_perm)
        else:
            out_rows["mapped_by"].append(pd.NA)
            out_rows["mapped_value"].append(pd.NA)
            out_rows["mapped_source"].append(pd.NA)
            out_rows["mapped_label"].append(pd.NA)
            out_rows["mapped_param"].append(pd.NA)
    return pd.DataFrame(out_rows, index=df_slice.index)
