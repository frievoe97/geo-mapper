"""Mapping pipeline step orchestrating multiple mappers.

Currently implements the first mapper: unique name match across loaded geodata.
Includes logging for visibility into mapping progress and outcomes.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from .mappers.sorted_tokens import sorted_tokens_mapper
from ..storage import (
    get_geodata_frames,
    get_selections,
    set_geodata_usage,
    set_geodata_mappings,
)
from .mappers.id_exact import id_exact_mapper
from .mappers.unique_name import unique_name_mapper
from .mappers.suffix_variants import (
    suffix_variants_mapper,
    set_used_ids_for_source as set_used_ids_for_suffix_variants,
)
from .mappers.regex_replace import (
    regex_replace_mapper,
    set_used_ids_for_source as set_used_ids_for_regex_replace,
)
from .mappers.fuzzy_confident import fuzzy_confident_mapper
from .mappers.token_permutation import token_permutation_mapper
from .selection import select_mappers_step
from ..utils.text import normalize_many
from ..constants import DEFAULT_MAPPERS

logger = logging.getLogger(__name__)


def _selected_mappers() -> list[str]:
    from ..storage import get_selected_mappers

    selected = get_selected_mappers()
    if not selected:
        # Default execution order if no mapper selection was stored
        return list(DEFAULT_MAPPERS)
    return selected


def mapping_step(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Apply mapping mappers sequentially and return the augmented DataFrame.

    Adds two columns:
    - mapped_by: which mapper produced the mapping
    - mapped_value: the mapped target value (e.g. geodata id)
    """

    selections = get_selections()
    # Initial log uses the legacy single column for backwards compatibility
    log_source_col = selections.column or dataframe.columns[0]

    geodata_frames: List[Tuple[Path, pd.DataFrame]] = get_geodata_frames()

    frames_with_id = sum(1 for _p, f in geodata_frames if "id" in f.columns)

    logger.info(
        "Mapping started for %d rows using source column '%s' against %d geodata frame(s)",
        len(dataframe),
        log_source_col,
        len(geodata_frames),
    )
    logger.info(
        "%d of %d geodata frame(s) contain required column 'id'",
        frames_with_id,
        len(geodata_frames),
    )
    if len(geodata_frames) == 0:
        logger.info("No geodata frames loaded. Skipping mapping.")
        return dataframe
    if frames_with_id == 0:
        logger.info("No geodata frames with an 'id' column found. Skipping mapping.")
        return dataframe



    # Build mapper list based on selection
    name_to_mapper = {
        "id_exact": id_exact_mapper,
        "unique_name": unique_name_mapper,
        "token_permutation": token_permutation_mapper,
        "regex_replace": regex_replace_mapper,
        "suffix_variants": suffix_variants_mapper,
        "fuzzy_confident": fuzzy_confident_mapper,
    }
    selected = [m for m in _selected_mappers() if m in name_to_mapper]
    if not selected:
        selected = list(DEFAULT_MAPPERS)

    total_rows = len(dataframe)

    # Track per-CSV cumulative matches for reporting (independent of DataFrame writes)
    # and per-CSV used geodata IDs to enforce 1:1 mappings.
    per_csv_cum: dict[str, set] = {str(p): set() for p, _ in geodata_frames}
    per_csv_used_ids: dict[str, set] = {str(p): set() for p, _ in geodata_frames}

    # Per-CSV mapping results: keep a separate mapping DataFrame per geodata CSV
    index = dataframe.index
    per_source_results: dict[str, pd.DataFrame] = {
        str(p): pd.DataFrame(
            {
                "mapped_by": pd.Series(pd.NA, index=index),
                "mapped_value": pd.Series(pd.NA, index=index),
                "mapped_source": pd.Series(pd.NA, index=index),
                "mapped_label": pd.Series(pd.NA, index=index),
                "mapped_param": pd.Series(pd.NA, index=index),
            }
        )
        for p, _ in geodata_frames
    }

    # Apply mappers only to currently-unmapped rows per geodata CSV.
    # One input row can therefore be mapped in several CSV files.
    for mapper_name in selected:
        mapper = name_to_mapper[mapper_name]
        selections = get_selections()

        # Choose the appropriate source column per mapper
        if mapper_name == "id_exact":
            source_col = selections.id_column
            if not source_col or source_col not in dataframe.columns:
                logger.info(
                    "Skipping mapper '%s' because no ID column is selected/available.",
                    mapper.__name__,
                )
                continue
        else:
            source_col = (
                selections.name_column
                or selections.column
                or dataframe.columns[0]
            )
            if source_col not in dataframe.columns:
                logger.info(
                    "Skipping mapper '%s' because source column '%s' is not in the DataFrame.",
                    mapper.__name__,
                    source_col,
                )
                continue

        logger.info("Running mapper '%s' on column '%s'", mapper.__name__, source_col)
        slice_cols = [source_col]
        if mapper_name != "id_exact" and "normalized_source" in dataframe.columns:
            slice_cols.append("normalized_source")

        # Potentials relative to full input (only meaningful for name-based mappers)
        if mapper_name != "id_exact":
            norm_values = (
                dataframe["normalized_source"].tolist()
                if "normalized_source" in dataframe.columns
                else normalize_many(dataframe[source_col])
            )
            from collections import Counter

            total_inputs = len(norm_values)
        else:
            total_inputs = len(dataframe)

        logger.info("Per-CSV results for %s:", mapper.__name__)
        for path, frame in geodata_frames:
            path_str = str(path)
            per_source_df = per_source_results[path_str]

            # For mappers that benefit from knowledge of already-used IDs
            # (e.g. regex_replace, suffix_variants), provide that information.
            if mapper_name == "suffix_variants":
                set_used_ids_for_suffix_variants(path_str, per_csv_used_ids[path_str])
            elif mapper_name == "regex_replace":
                set_used_ids_for_regex_replace(path_str, per_csv_used_ids[path_str])

            # Run this mapper against the full input for this single CSV
            full_updates = mapper(
                dataframe.loc[:, slice_cols].copy(), [(path, frame)], source_col
            )

            # Only consider rows that are currently still unmapped
            current_unmapped = per_source_df["mapped_value"].isna()
            target_index = full_updates.index.intersection(
                per_source_df.index[current_unmapped]
            )
            updates = full_updates.loc[target_index]

            # Per geodata CSV, ensure that each geodata ID is used at most once
            # so mappings stay one-to-one within a CSV.
            used_ids = per_csv_used_ids[path_str]
            effective_mapped_idx: set = set()
            if "mapped_value" in updates.columns:
                keep_indices: list = []
                for idx, gid in updates["mapped_value"].items():
                    if pd.isna(gid):
                        # No mapping for this row – does not affect used_ids
                        keep_indices.append(idx)
                        continue
                    gid_str = str(gid)
                    if gid_str in used_ids:
                        # This geodata ID has already been used for a different
                        # input row in this CSV – drop this mapping.
                        continue
                    used_ids.add(gid_str)
                    effective_mapped_idx.add(idx)
                    keep_indices.append(idx)
                updates = updates.loc[keep_indices]

            # New mapped indices for this CSV in this step (for statistics/logging)
            mapped_idx = effective_mapped_idx
            new_for_csv = mapped_idx - per_csv_cum[path_str]
            step_mapped = len(new_for_csv)
            per_csv_cum[path_str].update(mapped_idx)

            # Write into the per-CSV result, but only where no mapping exists yet
            for col in ["mapped_by", "mapped_value", "mapped_source", "mapped_label", "mapped_param"]:
                if col not in full_updates.columns:
                    continue
                per_source_df.loc[updates.index, col] = per_source_df.loc[
                    updates.index, col
                ].where(per_source_df.loc[updates.index, col].notna(), updates[col])

            # Compute potentials (number of input rows that would be uniquely
            # mappable by a strict normalized-name match across the full input)
            if mapper_name != "id_exact":
                if "name" in frame.columns:
                    norm_names = normalize_many(frame["name"])  # list[str]
                    name_counts = Counter(norm_names)
                    potential = sum(1 for v in norm_values if name_counts.get(v, 0) == 1)
                else:
                    potential = 0

            # Percentages relative to total input size
            new_cum_mapped = len(per_csv_cum[path_str])
            cum_pct = 0.0 if total_inputs == 0 else (new_cum_mapped / total_inputs * 100.0)
            step_pct = 0.0 if total_inputs == 0 else (step_mapped / total_inputs * 100.0)

            logger.info(
                "%s -> +%d (%.1f%%) this step; %d total (%.1f%%)",
                path_str,
                int(step_mapped),
                step_pct,
                int(new_cum_mapped),
                cum_pct,
            )

    # Persist per-CSV usage so export and interactive helpers can show coverage
    usage_counts = {path: len(indices) for path, indices in per_csv_cum.items()}
    used_ids_counts = {path: len(ids) for path, ids in per_csv_used_ids.items()}
    geodata_rows_by_source: dict[str, int] = {}
    for p, frame in geodata_frames:
        path_str = str(p)
        if "id" in frame.columns:
            geodata_rows_by_source[path_str] = int(frame["id"].notna().sum())
        else:
            geodata_rows_by_source[path_str] = int(len(frame))
    set_geodata_usage(usage_counts, total_rows, used_ids_counts, geodata_rows_by_source)

    # Persist per-CSV mapping DataFrames so the export step can later access
    # separate mappings for each geodata CSV.
    set_geodata_mappings(per_source_results)

    return dataframe
