"""Interactive selection of mapping steps to run."""

from __future__ import annotations

import pandas as pd
import questionary

from ..storage import get_selections, set_selected_mappers, get_auto_select_mappers
from ..constants import DEFAULT_MAPPERS, PROMPT_SELECT_MAPPERS
from ..utils.ui import DEFAULT_STYLE


def _default_mappers_for(selections) -> list[str]:
    """Return the default mapper list based on available columns."""
    id_columns = getattr(selections, "id_columns", []) or []
    has_id = bool(id_columns)
    effective_name = selections.name_column or selections.column
    has_name = effective_name is not None and effective_name not in set(id_columns)

    mappers: list[str] = []
    if has_id:
        mappers.extend(["exact_id", "id_without_leading_zero"])
    if has_name:
        mappers.extend(
            [
                "unique_name",
                "regex_replace",
                "token_permutation",
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

    id_columns = getattr(selections, "id_columns", []) or []
    has_id = bool(id_columns)
    effective_name = selections.name_column or selections.column
    has_name = effective_name is not None and effective_name not in set(id_columns)

    try:
        from questionary import Choice
    except ImportError:
        # Fallback if Choice is not available
        selected = questionary.checkbox(
            PROMPT_SELECT_MAPPERS,
            choices=list(DEFAULT_MAPPERS),
            style=DEFAULT_STYLE,
        ).ask()
    else:
        choices = [
            Choice("exact_id", checked=has_id),
            Choice("id_without_leading_zero", checked=has_id),
            Choice("unique_name", checked=has_name),
            Choice("regex_replace", checked=has_name),
            Choice("token_permutation", checked=has_name),
        ]
        selected = questionary.checkbox(
            PROMPT_SELECT_MAPPERS,
            choices=choices,
            style=DEFAULT_STYLE,
        ).ask()

    if not selected:
        # If nothing selected, default to all
        selected = list(DEFAULT_MAPPERS)
    set_selected_mappers(list(selected))
    return dataframe
