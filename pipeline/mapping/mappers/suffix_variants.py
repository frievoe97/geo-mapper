"""Mapper: try adding/removing simple title words, then normalize and match.

For each source value, build variants by:
- original value
- prefixing a suffix word (treated as title) once
- appending a suffix word once

Normalize each variant and check for an exact unique match within the given
geodata frame. If exactly one candidate is found for any one variant, map.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple
import logging

import pandas as pd

from ...utils.text import normalize_string
from ...constants import SUFFIX_TITLE_WORDS


logger = logging.getLogger(__name__)


TITLE_WORDS = list(SUFFIX_TITLE_WORDS)

# Optional: already-used geodata IDs per CSV, set by the orchestrator.
USED_IDS_BY_SOURCE: Dict[str, set[str]] = {}


def set_used_ids_for_source(source: str, ids: set) -> None:
    """Record already-used geodata IDs for a given CSV path."""

    USED_IDS_BY_SOURCE[source] = {str(gid) for gid in ids}


def _build_norm_lookup(frame: pd.DataFrame) -> Dict[str, List[Tuple[str, str]]]:
    """Map normalized name -> list of (id, original_name)."""
    lookup: Dict[str, List[Tuple[str, str]]] = {}
    for raw, gid in zip(frame.get("name", []), frame.get("id", []), strict=False):
        norm = normalize_string(raw)
        if not norm:
            continue
        lookup.setdefault(norm, []).append((str(gid), str(raw)))
    return lookup


def suffix_variants_mapper(
    df_slice: pd.DataFrame, geodata_frames: List[Tuple[Path, pd.DataFrame]], source_col: str
) -> pd.DataFrame:
    # Expect a single geodata frame per the orchestrator
    if not geodata_frames:
        return pd.DataFrame(
            index=df_slice.index,
            columns=["mapped_by", "mapped_value", "mapped_source", "mapped_label", "mapped_param"],
        ).assign(
            mapped_by=pd.NA,
            mapped_value=pd.NA,
            mapped_source=pd.NA,
            mapped_label=pd.NA,
            mapped_param=pd.NA,
        )
    csv_path, frame = geodata_frames[0]
    if not {"name", "id"}.issubset(frame.columns):
        return pd.DataFrame(
            index=df_slice.index,
            columns=["mapped_by", "mapped_value", "mapped_source", "mapped_label", "mapped_param"],
        ).assign(
            mapped_by=pd.NA,
            mapped_value=pd.NA,
            mapped_source=pd.NA,
            mapped_label=pd.NA,
            mapped_param=pd.NA,
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

        # Build variants: original + appended + prepended titles
        variants = [original]
        variants += [f"{original} {suf}" for suf in TITLE_WORDS]
        variants += [f"{suf} {original}" for suf in TITLE_WORDS]

        # Normalize each variant but keep mapping from normalized -> original variants (for mapped_param)
        norm_to_variants: dict[str, set[str]] = {}
        for v in variants:
            if not v:
                continue
            norm = normalize_string(v)
            if not norm:
                continue
            norm_to_variants.setdefault(norm, set()).add(v)

        # Collect variants that produce exactly one candidate (unique hit)
        hits: List[Tuple[str, str, str]] = []  # list of (norm_variant, candidate_id, candidate_label)
        for norm_variant in norm_to_variants:
            cands = lookup.get(norm_variant, [])
            # Filter out already-used IDs so that, for example, after mapping
            # \"Regensburg, Kreisfreie Stadt\" first, a second row can still
            # map uniquely to \"Regensburg, Landkreis\".
            available = [(gid, label) for gid, label in cands if str(gid) not in used_ids]
            if len(available) == 1:
                cand_id, cand_label = available[0]
                hits.append((norm_variant, cand_id, cand_label))
            # if len(cands) == 0 -> no hit for this variant
            # if len(cands) > 1 -> ambiguous; we do NOT count it as a valid unique hit

        # Only map if exactly ONE of the variants yielded a unique (single) candidate
        if len(hits) == 1:
            used_norm_variant, hit_id, hit_label = hits[0]
            out_rows["mapped_by"].append("suffix_variants")
            out_rows["mapped_value"].append(hit_id)
            out_rows["mapped_source"].append(str(csv_path))
            out_rows["mapped_label"].append(hit_label)
            # parameter: which normalized variant matched (incl. title word)
            out_rows["mapped_param"].append(used_norm_variant)
        else:
            # 0 hits or multiple hits -> do not map
            if len(hits) > 1:
                logger.debug(
                    "suffix_variants: multiple variant-hits for source=%s (index=%s): %s",
                    original,
                    i,
                    [(h[0], h[1]) for h in hits],
                )
            out_rows["mapped_by"].append(pd.NA)
            out_rows["mapped_value"].append(pd.NA)
            out_rows["mapped_source"].append(pd.NA)
            out_rows["mapped_label"].append(pd.NA)
            out_rows["mapped_param"].append(pd.NA)

    return pd.DataFrame(out_rows, index=df_slice.index)
