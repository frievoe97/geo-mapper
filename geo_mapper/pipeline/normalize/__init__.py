"""Pipeline step that computes and stores normalized strings for the source column."""

from __future__ import annotations

import pandas as pd

from ..storage import get_selections
from ..utils.text import normalize_many


def normalize_source_step(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Add a ``normalized_source`` column based on the selected source column.

    Keeps the original values intact; all mappers should rely on the normalized
    strings for comparisons.
    """
    selections = get_selections()
    # Prefer an explicit name column; fall back to the legacy single column.
    source_col = (
        selections.name_column
        or selections.column
        or dataframe.columns[0]
    )
    dataframe["normalized_source"] = normalize_many(dataframe[source_col])
    return dataframe
