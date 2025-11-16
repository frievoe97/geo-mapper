"""Pipeline step that keeps only the user-selected ID/name/value columns."""

from __future__ import annotations

import pandas as pd
import questionary

from ..storage import (
    set_column,
    set_id_column,
    set_name_column,
    set_value_columns,
    get_selections,
)


def _first_non_empty_value(series: pd.Series) -> str | None:
    """Return the first non-empty (non-NaN, non-whitespace) cell from a Series."""
    for value in series:
        if pd.isna(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return str(value)
    return None


def _build_column_choices(dataframe: pd.DataFrame) -> list[questionary.Choice]:
    """Build interactive choices using the column name and a sample value."""
    choices: list[questionary.Choice] = []
    for col in dataframe.columns:
        sample = _first_non_empty_value(dataframe[col])
        if sample is not None:
            title = f"{col} (example: {sample})"
        else:
            title = str(col)
        choices.append(questionary.Choice(title=title, value=col))
    return choices


def _choose_columns(
    dataframe: pd.DataFrame,
) -> tuple[str | None, str | None, list[str]]:
    """Prompt which ID, name and value columns should remain in the DataFrame.

    At least one of ID or name must be selected. Value columns are optional and
    may be multiple, but cannot re-use the chosen ID/name columns.
    """
    if dataframe.columns.empty:
        raise ValueError("The CSV contains no columns.")
    columns = list(dataframe.columns)
    if not columns:
        raise ValueError("The CSV contains no columns.")

    column_choices = _build_column_choices(dataframe)

    id_none_label = "<Do not use an ID column>"
    name_none_label = "<Do not use a name column>"

    # The 'none/unknown' options should always be shown first and be selected by default.
    id_default = id_none_label
    name_default = name_none_label

    id_choices = [
        questionary.Choice(title=id_none_label, value=id_none_label),
        *column_choices,
    ]
    name_choices = [
        questionary.Choice(title=name_none_label, value=name_none_label),
        *column_choices,
    ]

    id_column = questionary.select(
        "Which column contains the IDs?",
        choices=id_choices,
        default=id_default,
    ).ask()
    name_column = questionary.select(
        "Which column contains the names?",
        choices=name_choices,
        default=name_default,
    ).ask()

    if not id_column and not name_column:
        raise SystemExit("No column was selected.")

    if id_column == id_none_label:
        id_column = None
    if name_column == name_none_label:
        name_column = None

    if id_column is None and name_column is None:
        raise SystemExit(
            "At least one of 'ID' or 'name' must be selected."
        )

    # Value columns: all remaining columns, optional and possibly multiple
    remaining_for_values = [
        choice for choice in column_choices
        if choice.value not in {id_column, name_column}
    ]
    value_columns: list[str] = []
    if remaining_for_values:
        value_columns = list(questionary.checkbox(
            "Which columns contain values that should be included in the export? (optional)",
            choices=remaining_for_values,
        ).ask() or [])

    return id_column, name_column, list(value_columns)


def narrow_to_single_column_step(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Reduce the DataFrame to the chosen ID/name/value columns."""
    # If a meta configuration from JSON is present and valid, prefer those column choices.
    selections = get_selections()
    meta = getattr(selections, "meta_config", None)

    if isinstance(meta, dict):
        id_column_meta = meta.get("id_column")
        name_column_meta = meta.get("name_column")
        value_columns_meta = meta.get("value_columns")
        if isinstance(value_columns_meta, str):
            value_columns_meta = [value_columns_meta]
        elif not isinstance(value_columns_meta, list):
            value_columns_meta = []

        columns = list(dataframe.columns)
        valid = True
        if id_column_meta is not None and id_column_meta not in columns:
            valid = False
        if name_column_meta is not None and name_column_meta not in columns:
            valid = False
        if id_column_meta is None and name_column_meta is None:
            valid = False

        if valid:
            id_column = id_column_meta
            name_column = name_column_meta
            value_columns = [
                c
                for c in value_columns_meta
                if c in columns and c not in {id_column, name_column}
            ]
        else:
            id_column, name_column, value_columns = _choose_columns(dataframe)
    else:
        id_column, name_column, value_columns = _choose_columns(dataframe)

    # Persist selections
    set_id_column(id_column)
    if name_column is not None:
        # Keep legacy 'column' in sync with the name column
        set_name_column(name_column)
    elif id_column is not None:
        # Fallback: if only an ID column is used, treat it as source column
        set_column(id_column)

    # Persist value columns (export only)
    set_value_columns(value_columns)

    # Keep only the selected columns (deduplicated)
    selected_cols: list[str] = []
    for col in (id_column, name_column, *value_columns):
        if col is not None and col not in selected_cols:
            selected_cols.append(col)

    return dataframe[selected_cols].copy()
