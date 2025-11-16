"""Interactive selection of a single geodata CSV for manual mapping and export."""

from __future__ import annotations

import logging
from typing import List, Tuple

import pandas as pd
import questionary

from .storage import (
    get_geodata_geocoverage,
    get_geodata_usage,
    set_export_geodata_source,
    get_auto_export_source,
)
from .constants import PROMPT_SELECT_EXPORT_SOURCE


logger = logging.getLogger(__name__)


def _geodata_usage(dataframe: pd.DataFrame) -> List[Tuple[str, int, float, int, float, int]]:
    """Return per-geodata usage as (source_path, hits, pct_input, geodata_hits, pct_geodata, geodata_total_rows).

    Prefer the per-CSV cumulative counts recorded during the mapping step so
    that the values shown here match the logging output exactly. This mirrors
    the statistics logged in the mapping step.
    """
    usage_counts, total_rows = get_geodata_usage()
    used_ids_by_source, geodata_rows_by_source = get_geodata_geocoverage()
    if usage_counts and total_rows:
        usage: List[Tuple[str, int, float, int, float, int]] = []
        for source, hits in usage_counts.items():
            if hits <= 0:
                continue
            pct = float(hits) / float(total_rows) * 100.0
            used_ids = int(used_ids_by_source.get(source, 0))
            total_geodata_rows = int(geodata_rows_by_source.get(source, 0) or 0)
            pct_geodata = (
                float(used_ids) / float(total_geodata_rows) * 100.0
                if total_geodata_rows > 0
                else 0.0
            )
            usage.append((str(source), int(hits), pct, used_ids, pct_geodata, total_geodata_rows))
        # Sort order for sources:
        # 1. Descending by share of mapped input rows (pct_input, t[2])
        # 2. Then descending by share of used geodata rows (pct_geodata, t[4])
        # 3. Then descending by absolute geodata hits (geodata_hits, t[3])
        usage.sort(key=lambda t: (t[2], t[4], t[3]), reverse=True)
        return usage

    # Fallback: nothing recorded – no meaningful ranking is possible
    return []


def _choose_single_geodata_source(usage: List[Tuple[str, int, float, int, float, int]]) -> str | None:
    """Ask the user to select exactly one geodata source."""
    if not usage:
        return None

    try:
        from questionary import Choice

        choices = []
        for source, hits, pct_input, geodata_hits, pct_geodata, geodata_total in usage:
            title = (
                f"{source} "
                f"({hits} matches, {pct_input:.1f}% of input rows; "
                f"{geodata_hits} of {geodata_total} geodata rows, "
                f"{pct_geodata:.1f}% of geodata rows)"
            )
            choices.append(Choice(title=title, value=source))
        selected = questionary.select(
            PROMPT_SELECT_EXPORT_SOURCE,
            choices=choices,
        ).ask()
    except Exception:
        # Fallback without Choice helper: simple labels only
        selected = questionary.select(
            PROMPT_SELECT_EXPORT_SOURCE,
            choices=[source for source, _hits, _pct in usage],
        ).ask()

    return selected or None


def select_export_geodata_step(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Pipeline step: select a single geodata CSV for downstream steps."""
    usage = _geodata_usage(dataframe)
    if not usage:
        logger.info(
            "No mapped rows found – no single geodata source will be selected for export.",
        )
        set_export_geodata_source(None)
        return dataframe

    selected: str | None
    if get_auto_export_source():
        # Automatically use the first (already quality-sorted) geodata source.
        selected = usage[0][0]
        logger.info(
            "Geodata source chosen automatically (first in the sorted list): %s",
            selected,
        )
    else:
        selected = _choose_single_geodata_source(usage)
    if not selected:
        logger.info("No geodata source selected – export and manual mapping will be skipped.")
        set_export_geodata_source(None)
        return dataframe

    set_export_geodata_source(selected)
    logger.info("Geodata source for export/manual mapping: %s", selected)
    return dataframe
