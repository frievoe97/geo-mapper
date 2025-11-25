"""Second mapper: token-sorted exact matching on normalized strings.

Approach:
- For each geodata name, normalize it, split on whitespace, sort tokens, and
  re-join into a canonical key.
- Do the same for source values (prefer the precomputed 'normalized_source').
- Map when the canonical key is unique across all loaded geodata frames, or
  when it appears across multiple frames but always with the same ID (choose
  the newest version's CSV as the representative source path).
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from ...utils.text import normalize_string


logger = logging.getLogger(__name__)


def _token_key(text: str) -> str:
    if not text:
        return ""
    toks = text.split()
    toks.sort()
    return " ".join(toks)


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
    name_counts: Counter = Counter()
    hits_by_key: Dict[str, List[Tuple[str, Path, str]]] = defaultdict(list)
    frames_with_required = 0
    frames_indexed = len(geodata_frames)
    for csv_path, frame in geodata_frames:
        if not {"name", "id"}.issubset(frame.columns):
            continue
        frames_with_required += 1
        for name, gid in zip(frame["name"], frame["id"], strict=False):
            n = normalize_string(name)
            k = _token_key(n)
            name_counts[k] += 1
            hits_by_key[k].append((str(gid), csv_path, str(name)))

    def _version_key(p: Path) -> int:
        try:
            return int(p.parent.name)
        except (TypeError, ValueError):
            return -1

    unique_map_by_key: Dict[str, Tuple[str, Path]] = {}
    label_by_key: Dict[str, str] = {}
    # Strictly unique across all frames
    for k, count in name_counts.items():
        if count == 1 and len(hits_by_key[k]) == 1:
            gid, csv_path, label = hits_by_key[k][0]
            unique_map_by_key[k] = (gid, csv_path)
            label_by_key[k] = label
    # Consistent ID across frames → accept, choose newest version
    for k, triples in hits_by_key.items():
        if k in unique_map_by_key:
            continue
        ids = {t[0] for t in triples}
        if len(ids) == 1:
            gid = next(iter(ids))
            best = max(triples, key=lambda t: _version_key(t[1]))
            unique_map_by_key[k] = (gid, best[1])
            label_by_key[k] = best[2]

    logger.debug(
        "Token-key index: %d frame(s); %d with required; %d unique keys",
        frames_indexed,
        frames_with_required,
        len(unique_map_by_key),
    )
    return (
        name_counts,
        unique_map_by_key,
        hits_by_key,
        label_by_key,
        frames_indexed,
        frames_with_required,
    )


# def sorted_tokens_mapper(
#     df_slice: pd.DataFrame, geodata_frames: List[Tuple[Path, pd.DataFrame]], source_col: str
# ) -> pd.DataFrame:
#     (
#         name_counts,
#         unique_map_by_key,
#         hits_by_key,
#         label_by_key,
#         frames_indexed,
#         frames_with_required,
#     ) = _collect_geodata_index(geodata_frames)
#
#     mapped_by: List[object] = []
#     mapped_value: List[object] = []
#     mapped_source: List[object] = []
#     mapped_label: List[object] = []
#     mapped_param: List[object] = []
#     idx: List[object] = []
#     no_match_samples: List[str] = []
#     ambiguous_samples: List[str] = []
#     attempt_counter = 0
#
#     # Prefer precomputed normalization if available
#     norm_series = (
#         df_slice["normalized_source"]
#         if "normalized_source" in df_slice.columns
#         else df_slice[source_col].map(normalize_string)
#     )
#     key_series = norm_series.map(_token_key)
#
#     for i in df_slice.index:
#         original = df_slice.at[i, source_col]
#         key = key_series.at[i]
#         attempt_counter += 1
#         hit = unique_map_by_key.get(key)
#         if hit is not None:
#             gid, csv_path = hit
#             if attempt_counter % 100 == 0:
#                 logger.debug(
#                     "attempt %d: token-sort source=%r -> key=%r -> match id=%s (source=%s)",
#                     attempt_counter,
#                     original,
#                     key,
#                     gid,
#                     str(csv_path),
#                 )
#             mapped_by.append("sorted_tokens")
#             mapped_value.append(gid)
#             mapped_source.append(str(csv_path))
#             mapped_label.append(label_by_key.get(key, pd.NA))
#             # parameter: the token key that was used
#             mapped_param.append(key)
#         else:
#             count = name_counts.get(key, 0)
#             if count == 0:
#                 if attempt_counter % 100 == 0:
#                     logger.debug(
#                         "attempt %d: token-sort source=%r -> key=%r -> no-match",
#                         attempt_counter,
#                         original,
#                         key,
#                     )
#                 if len(no_match_samples) < 5:
#                     no_match_samples.append(str(original))
#             else:
#                 ids_count = len({t[0] for t in hits_by_key.get(key, [])})
#                 if attempt_counter % 100 == 0:
#                     logger.debug(
#                         "attempt %d: token-sort source=%r -> key=%r -> ambiguous matches=%d ids=%d",
#                         attempt_counter,
#                         original,
#                         key,
#                         count,
#                         ids_count,
#                     )
#                 if len(ambiguous_samples) < 5:
#                     ambiguous_samples.append(
#                         f"{original} (matches={count}, ids={ids_count})"
#                     )
#             mapped_by.append(pd.NA)
#             mapped_value.append(pd.NA)
#             mapped_source.append(pd.NA)
#             mapped_label.append(pd.NA)
#             mapped_param.append(pd.NA)
#         idx.append(i)
#
#     out = pd.DataFrame(
#         {
#             "mapped_by": mapped_by,
#             "mapped_value": mapped_value,
#             "mapped_source": mapped_source,
#             "mapped_label": mapped_label,
#             "mapped_param": mapped_param,
#         },
#         index=idx,
#     )
#     mapped_count = int(out["mapped_value"].notna().sum())
#     total = len(out)
#     logger.debug(
#         "sorted_tokens result: %d/%d mapped; frames indexed=%d, with ['name','id']=%d",
#         mapped_count,
#         total,
#         frames_indexed,
#         frames_with_required,
#     )
#     remaining = total - mapped_count
#     if remaining:
#         no_match_count = 0
#         ambiguous_count = 0
#         for i, val in df_slice[source_col].items():
#             if pd.isna(out.loc[i, "mapped_value"]):
#                 k = _token_key(normalize_string(val))
#                 c = name_counts.get(k, 0)
#                 if c == 0:
#                     no_match_count += 1
#                 else:
#                     ambiguous_count += 1
#         logger.debug(
#             "sorted_tokens could not map %d row(s): %d no-match, %d ambiguous",
#             remaining,
#             int(no_match_count),
#             int(ambiguous_count),
#         )
#         if no_match_samples:
#             logger.debug("No-match examples (up to 5): %s", ", ".join(no_match_samples))
#         if ambiguous_samples:
#             logger.debug("Ambiguous examples (up to 5): %s", ", ".join(ambiguous_samples))
#     else:
#         logger.debug("All rows mapped by sorted_tokens mapper")
#     return out
