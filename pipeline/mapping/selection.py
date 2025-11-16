"""Interactive selection of mapping steps to run."""

from __future__ import annotations

import pandas as pd
import questionary

from ..storage import get_selections, set_selected_mappers, get_auto_select_mappers
from ..constants import DEFAULT_MAPPERS, PROMPT_SELECT_MAPPERS


def _default_mappers_for(selections) -> list[str]:
    """Return the default mapper list based on available columns."""
    has_id = selections.id_column is not None
    has_name = selections.name_column is not None or selections.column is not None

    mappers: list[str] = []
    if has_id:
        mappers.append("id_exact")
    if has_name:
        mappers.extend(
            [
                "unique_name",
                "token_permutation",
                "regex_replace",
                "suffix_variants",
                "fuzzy_confident",
            ]
        )
    # Fallback: if for some reason no mapper was selected, use all defaults
    return mappers or list(DEFAULT_MAPPERS)


def select_mappers_step(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Ask which mapping steps to execute (default: all or auto-select)."""
    selections = get_selections()

    # If configured via CLI, skip interactive selection and use defaults.
    if get_auto_select_mappers():
        selected = _default_mappers_for(selections)
        set_selected_mappers(selected)
        return dataframe

    has_id = selections.id_column is not None
    has_name = selections.name_column is not None or selections.column is not None

    try:
        from questionary import Choice
    except ImportError:
        # Fallback if Choice is not available
        selected = questionary.checkbox(
            PROMPT_SELECT_MAPPERS,
            choices=list(DEFAULT_MAPPERS),
        ).ask()
    else:
        choices = [
            Choice("id_exact", checked=has_id),
            Choice("unique_name", checked=has_name),
            Choice("token_permutation", checked=has_name),
            Choice("regex_replace", checked=has_name),
            Choice("suffix_variants", checked=has_name),
            Choice("fuzzy_confident", checked=has_name),
        ]
        selected = questionary.checkbox(
            PROMPT_SELECT_MAPPERS,
            choices=choices,
        ).ask()

    if not selected:
        # If nothing selected, default to all
        selected = list(DEFAULT_MAPPERS)
    set_selected_mappers(list(selected))
    return dataframe
