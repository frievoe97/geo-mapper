"""ID-exact mapper: map when a normalized ID matches uniquely in geodata.

The mapper:
- normalizes IDs in both input and geodata using ``normalize_id``
- builds an index over all loaded geodata frames
- maps a row if its normalized ID occurs unambiguously in geodata
  (unique across all frames or consistently pointing to the same ID)
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd

from pipeline.utils.text import normalize_id


logger = logging.getLogger(__name__)


def _collect_geodata_id_index(
    geodata_frames: List[Tuple[Path, pd.DataFrame]]
) -> Tuple[
    Counter,
    Dict[str, Tuple[str, Path]],
    Dict[str, List[Tuple[str, Path, str]]],
    Dict[str, str],
    int,
    int,
]:
    """Build indexes for normalized IDs across all frames.

    Returns:
    - id_counts: Counter of occurrences per normalized id across all frames
    - unique_map_by_id: ids that resolve unambiguously mapped to (id, csv_path)
    - hits_by_id: mapping to all hits (id, csv_path, original_name) for diagnostics
    - label_by_id: for ids that resolve uniquely, their original geodata label
    - frames_indexed: total frames processed
    - frames_with_required: frames that contained an 'id' column
    """
    id_counts: Counter = Counter()
    hits_by_id: Dict[str, List[Tuple[str, Path, str]]] = defaultdict(list)
    frames_with_required = 0
    frames_indexed = len(geodata_frames)

    for csv_path, frame in geodata_frames:
        if "id" not in frame.columns:
            continue
        frames_with_required += 1

        # Use name column if available, otherwise empty label
        name_series = frame["name"].astype(str) if "name" in frame.columns else pd.Series(
            ["" for _ in range(len(frame))], index=frame.index
        )

        for gid, name in zip(frame["id"], name_series, strict=False):
            norm_id = normalize_id(gid)
            if not norm_id:
                continue
            gid_str = str(gid)
            id_counts[norm_id] += 1
            hits_by_id[norm_id].append((gid_str, csv_path, str(name)))

    def _version_key(p: Path) -> int:
        try:
            return int(p.parent.name)
        except Exception:
            return -1

    unique_map_by_id: Dict[str, Tuple[str, Path]] = {}
    label_by_id: Dict[str, str] = {}

    # First pass: strictly unique normalized IDs across all frames
    for n, count in id_counts.items():
        if count == 1 and len(hits_by_id[n]) == 1:
            gid, csv_path, label = hits_by_id[n][0]
            unique_map_by_id[n] = (gid, csv_path)
            label_by_id[n] = label

    # Second pass: IDs that appear multiple times but always with the same gid;
    # choose the newest version path as representative.
    for n, triples in hits_by_id.items():
        if n in unique_map_by_id:
            continue
        ids = {t[0] for t in triples}
        if len(ids) == 1:
            gid = next(iter(ids))
            best = max(triples, key=lambda t: _version_key(t[1]))
            unique_map_by_id[n] = (gid, best[1])
            label_by_id[n] = best[2]

    logger.debug(
        "Indexed %d geodata frame(s); %d with required column 'id'; %d unique normalized IDs available",
        frames_indexed,
        frames_with_required,
        len(unique_map_by_id),
    )
    return (
        id_counts,
        unique_map_by_id,
        hits_by_id,
        label_by_id,
        frames_indexed,
        frames_with_required,
    )


def id_exact_mapper(
    df_slice: pd.DataFrame, geodata_frames: List[Tuple[Path, pd.DataFrame]], source_col: str
) -> pd.DataFrame:
    """Map rows where the ID value can be resolved uniquely across all geodata."""
    (
        id_counts,
        unique_map_by_id,
        hits_by_id,
        label_by_id,
        frames_indexed,
        frames_with_required,
    ) = _collect_geodata_id_index(geodata_frames)

    mapped_by: List[object] = []
    mapped_value: List[object] = []
    mapped_source: List[object] = []
    mapped_label: List[object] = []
    idx: List[object] = []
    no_match_samples: List[str] = []
    ambiguous_samples: List[str] = []

    attempt_counter = 0

    for i in df_slice.index:
        original = df_slice.at[i, source_col]
        norm_val = normalize_id(original)
        attempt_counter += 1

        hit = unique_map_by_id.get(norm_val)
        if hit is not None:
            gid, csv_path = hit
            mapped_by.append("id_exact")
            mapped_value.append(gid)
            mapped_source.append(str(csv_path))
            mapped_label.append(label_by_id.get(norm_val, pd.NA))
        else:
            count = id_counts.get(norm_val, 0)
            if count == 0:
                if len(no_match_samples) < 5:
                    no_match_samples.append(str(original))
            else:
                ids_count = len({t[0] for t in hits_by_id.get(norm_val, [])})
                if attempt_counter % 100 == 0:
                    logger.debug(
                        "id_exact attempt %d: source=%r | normalized=%r -> ambiguous matches=%d ids=%d",
                        attempt_counter,
                        original,
                        norm_val,
                        count,
                        ids_count,
                    )
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
        "id_exact result: %d/%d mapped; frames indexed=%d, with 'id'=%d",
        mapped_count,
        total,
        frames_indexed,
        frames_with_required,
    )
    remaining = total - mapped_count
    if remaining:
        no_match_count = 0
        ambiguous_count = 0
        for i, val in df_slice[source_col].items():
            if pd.isna(out.loc[i, "mapped_value"]):
                n = normalize_id(val)
                c = id_counts.get(n, 0)
                if c == 0:
                    no_match_count += 1
                else:
                    ambiguous_count += 1
        logger.debug(
            "id_exact could not map %d row(s): %d no-match, %d ambiguous",
            remaining,
            int(no_match_count),
            int(ambiguous_count),
        )
        if no_match_samples:
            logger.debug("ID no-match examples (up to 5): %s", ", ".join(no_match_samples))
        if ambiguous_samples:
            logger.debug(
                "ID ambiguous examples (up to 5): %s", ", ".join(ambiguous_samples)
            )
    else:
        logger.debug("All rows mapped by id_exact mapper")

    return out

