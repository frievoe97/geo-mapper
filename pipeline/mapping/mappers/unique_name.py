"""Unique-name mapper: map when exactly one geodata row matches the normalized name.

Normalization rules before comparing names:
- lower case only (casefold)
- replace German umlauts (ä→ae, ö→oe, ü→ue; ß handled by casefold→ss)
- digits are removed; punctuation and special characters become spaces
- collapse multiple spaces to a single space

The actual normalization is centralized in ``pipeline.utils.text.normalize_string``.
If the input slice already contains a ``normalized_source`` column, it is used
directly to avoid double work; otherwise values are normalized on the fly.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd

from pipeline.utils.text import normalize_string

logger = logging.getLogger(__name__)




def _collect_geodata_index(
    geodata_frames: List[Tuple[Path, pd.DataFrame]]
) -> Tuple[
    Counter,
    Dict[str, Tuple[str, Path]],
    Dict[str, List[Tuple[str, Path, str]]],
    Dict[str, str],
    int,
    int,
]:
    """Build indexes for normalized names across all frames.

    Returns:
    - name_counts: Counter of occurrences per normalized name across all frames
    - unique_map_by_name: names that appear exactly once overall mapped to (id, csv_path)
    - hits_by_name: mapping to all hits (id, csv_path, original_name) for diagnostics
    - label_by_name: for names that are unique overall, their original geodata label
    - frames_indexed: total frames processed
    - frames_with_required: frames that contained ['name', 'id']
    """
    name_counts: Counter = Counter()
    hits_by_name: Dict[str, List[Tuple[str, Path, str]]] = defaultdict(list)
    frames_with_required = 0
    frames_indexed = len(geodata_frames)
    for csv_path, frame in geodata_frames:
        # Expect 'name' and 'id' columns in geodata
        if not {"name", "id"}.issubset(frame.columns):
            continue
        frames_with_required += 1
        for name, gid in zip(frame["name"], frame["id"], strict=False):
            n = normalize_string(name)
            name_counts[n] += 1
            hits_by_name[n].append((str(gid), csv_path, str(name)))

    def _version_key(p: Path) -> int:
        try:
            return int(p.parent.name)
        except Exception:
            return -1

    unique_map_by_name: Dict[str, Tuple[str, Path]] = {}
    label_by_name: Dict[str, str] = {}
    # First pass: strictly unique across all frames
    for n, count in name_counts.items():
        if count == 1 and len(hits_by_name[n]) == 1:
            gid, csv_path, label = hits_by_name[n][0]
            unique_map_by_name[n] = (gid, csv_path)
            label_by_name[n] = label
    # Second pass: consistent id across frames (ids_count == 1)
    for n, triples in hits_by_name.items():
        if n in unique_map_by_name:
            continue
        ids = {t[0] for t in triples}
        if len(ids) == 1:
            gid = next(iter(ids))
            # choose the newest version path as representative
            best = max(triples, key=lambda t: _version_key(t[1]))
            unique_map_by_name[n] = (gid, best[1])
            label_by_name[n] = best[2]
    logger.debug(
        "Indexed %d geodata frame(s); %d with required columns; %d unique normalized names available",
        frames_indexed,
        frames_with_required,
        len(unique_map_by_name),
    )
    return (
        name_counts,
        unique_map_by_name,
        hits_by_name,
        label_by_name,
        frames_indexed,
        frames_with_required,
    )


def unique_name_mapper(
    df_slice: pd.DataFrame, geodata_frames: List[Tuple[Path, pd.DataFrame]], source_col: str
) -> pd.DataFrame:
    """Map rows where the source value appears exactly once across all geodata.

    Returns a DataFrame containing 'mapped_by', 'mapped_value' and 'mapped_source'
    with the same index as `df_slice`.
    """
    (
        name_counts,
        unique_map_by_name,
        hits_by_name,
        label_by_name,
        frames_indexed,
        frames_with_required,
    ) = _collect_geodata_index(geodata_frames)

    mapped_by: List[object] = []
    mapped_value: List[object] = []
    mapped_source: List[object] = []
    mapped_label: List[object] = []
    idx: List[object] = []
    no_match_samples: List[str] = []
    ambiguous_samples: List[str] = []
    attempt_counter = 0

    def _lookup_geodata_name(
        frames: List[Tuple[Path, pd.DataFrame]],
        match_path: Path,
        match_id: str,
    ) -> Optional[str]:
        """Try to find the original geodata 'name' for logging purposes.

        This is only called for every 100th attempt, so a linear scan is fine.
        """
        for p, f in frames:
            if p == match_path and {"name", "id"}.issubset(f.columns):
                # compare as strings for robustness
                hits = f.loc[f["id"].astype(str) == str(match_id), "name"]
                if not hits.empty:
                    return str(hits.iloc[0])
        return None

    # Prefer precomputed normalization if available
    norm_series = (
        df_slice["normalized_source"]
        if "normalized_source" in df_slice.columns
        else df_slice[source_col].map(normalize_string)
    )

    for i in df_slice.index:
        original = df_slice.at[i, source_col]
        norm_val = norm_series.at[i]
        attempt_counter += 1
        hit = unique_map_by_name.get(norm_val)
        if hit is not None:
            gid, csv_path = hit
            mapped_by.append("unique_name")
            mapped_value.append(gid)
            mapped_source.append(str(csv_path))
            mapped_label.append(label_by_name.get(norm_val, pd.NA))
        else:
            count = name_counts.get(norm_val, 0)
            if count == 0:
                # if attempt_counter % 100 == 0:
                #     logger.info(
                #         "attempt %d: source=%r | normalized=%r -> no-match",
                #         attempt_counter,
                #         original,
                #         norm_val,
                #     )
                # logger.info(
                #     "unique_name attempt idx=%s value=%r norm=%r -> no-match",
                #     i,
                #     val,
                #     norm_val,
                # )
                if len(no_match_samples) < 5:
                    no_match_samples.append(str(original))
            else:
                ids_count = len({t[0] for t in hits_by_name.get(norm_val, [])})
                if attempt_counter % 100 == 0:
                    logger.debug(
                        "attempt %d: source=%r | normalized=%r -> ambiguous matches=%d ids=%d",
                        attempt_counter,
                        original,
                        norm_val,
                        count,
                        ids_count,
                    )
                # logger.info(
                #     "unique_name attempt idx=%s value=%r norm=%r -> ambiguous matches=%d ids=%d",
                #     i,
                #     val,
                #     norm_val,
                #     count,
                #     ids_count,
                # )
                if len(ambiguous_samples) < 5:
                    ambiguous_samples.append(
                        f"{original} (matches={count}, ids={ids_count})"
                    )
            mapped_by.append(pd.NA)
            mapped_value.append(pd.NA)
            mapped_source.append(pd.NA)
            mapped_label.append(pd.NA)
        idx.append(i)

    out = pd.DataFrame(
        {
            "mapped_by": mapped_by,
            "mapped_value": mapped_value,
            "mapped_source": mapped_source,
            "mapped_label": mapped_label,
        },
        index=idx,
    )
    mapped_count = int(out["mapped_value"].notna().sum())
    total = len(out)
    logger.debug(
        "unique_name result: %d/%d mapped; frames indexed=%d, with ['name','id']=%d",
        mapped_count,
        total,
        frames_indexed,
        frames_with_required,
    )
    remaining = total - mapped_count
    if remaining:
        # Summarize reasons
        no_match_count = 0
        ambiguous_count = 0
        for i, val in df_slice[source_col].items():
            if pd.isna(out.loc[i, "mapped_value"]):
                n = normalize_string(val)
                c = name_counts.get(n, 0)
                if c == 0:
                    no_match_count += 1
                else:
                    ambiguous_count += 1
        logger.debug(
            "unique_name could not map %d row(s): %d no-match, %d ambiguous",
            remaining,
            int(no_match_count),
            int(ambiguous_count),
        )
        if no_match_samples:
            logger.debug("No-match examples (up to 5): %s", ", ".join(no_match_samples))
        if ambiguous_samples:
            logger.debug("Ambiguous examples (up to 5): %s", ", ".join(ambiguous_samples))
    else:
        logger.debug("All rows mapped by unique_name mapper")
    return out
