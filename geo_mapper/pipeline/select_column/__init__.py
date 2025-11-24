"""Pipeline step that keeps only the user-selected ID/name/value columns."""

from __future__ import annotations

import pandas as pd
import questionary

from ..storage import (
    set_column,
    set_id_columns,
    set_name_column,
    set_value_columns,
    get_selections,
)
from ..constants import (
    PROMPT_SELECT_ID_COLUMN,
    PROMPT_SELECT_NAME_COLUMN,
    PROMPT_SELECT_VALUE_COLUMNS,
    ID_NONE_LABEL,
    NAME_NONE_LABEL,
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


def _sanitize_for_display(text: str) -> str:
    """Return a compact display string without line breaks/spaces and at most 15 chars."""
    cleaned = text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    cleaned = cleaned.replace(" ", "")
    limit = 25
    if len(cleaned) > limit:
        cleaned = cleaned[: limit - 3] + "..."
    return cleaned


def _first_non_empty_values(series: pd.Series, max_examples: int = 3) -> list[str]:
    """Return up to max_examples non-empty (non-NaN, non-whitespace) cells from a Series."""
    values: list[str] = []
    for value in series:
        if pd.isna(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        values.append(str(value))
        if len(values) >= max_examples:
            break
    return values


def _build_column_choices(dataframe: pd.DataFrame) -> list[questionary.Choice]:
    """Build interactive choices using the column name and a sample value."""
    choices: list[questionary.Choice] = []
    for col in dataframe.columns:
        samples = _first_non_empty_values(dataframe[col], max_examples=3)
        # If the column has no non-empty cells, fall back to using the
        # column header itself as the "example" value so that *always*
        # some representative value is shown.
        if not samples:
            samples = [str(col)]
        display_col = _sanitize_for_display(str(col))
        display_examples = ", ".join(_sanitize_for_display(str(sample)) for sample in samples)
        title = f"{display_col} (examples: {display_examples})"
        choices.append(questionary.Choice(title=title, value=col))
    return choices


def _prompt_value_renames(
    value_columns: list[str],
    id_columns: list[str],
    name_column: str | None,
) -> dict[str, str]:
    """Ask for optional renames per selected value column.

    Returns a mapping of original column name -> new column name.
    Ensures uniqueness across ID/name/value columns by appending counters when needed.
    """
    if not value_columns:
        return {}

    used_names: set[str] = set(id_columns)
    if name_column:
        used_names.add(name_column)

    renames: dict[str, str] = {}
    for col in value_columns:
        new_name = questionary.text(
            f"Neuer Name für '{col}' (leer lassen für unverändert):",
            default=col,
        ).ask()
        normalized = (new_name or col).strip() or col
        base = normalized
        candidate = base
        suffix = 2
        while candidate in used_names:
            candidate = f"{base}_{suffix}"
            suffix += 1
        renames[col] = candidate
        used_names.add(candidate)
    return renames


def _choose_columns(
    dataframe: pd.DataFrame,
) -> tuple[list[str], str | None, list[str], dict[str, str]]:
    """Prompt which ID, name and value columns should remain in the DataFrame.

    At least one of ID or name must be selected. Value columns are optional and
    may be multiple, but cannot re-use the chosen ID/name columns.
    """
    if dataframe.columns.empty:
        raise ValueError("The input data contains no columns.")

    column_choices = _build_column_choices(dataframe)

    id_none_label = ID_NONE_LABEL
    name_none_label = NAME_NONE_LABEL

    # The 'none/unknown' options should always be shown first and be selected by default.
    id_default = id_none_label
    name_default = name_none_label

    id_choices = [
        questionary.Choice(title=id_none_label, value=id_none_label),
        *column_choices,
    ]
    id_selection = questionary.checkbox(
        PROMPT_SELECT_ID_COLUMN,
        choices=id_choices,
    ).ask() or []
    if id_none_label in id_selection:
        id_columns = []
    else:
        id_columns = [col for col in id_selection if col != id_none_label]

    name_choices = [
        questionary.Choice(title=name_none_label, value=name_none_label),
        *[
            choice
            for choice in column_choices
            if choice.value not in set(id_columns)
        ],
    ]
    name_column = questionary.select(
        PROMPT_SELECT_NAME_COLUMN,
        choices=name_choices,
        default=name_default,
    ).ask()

    if not id_columns and not name_column:
        raise SystemExit("No column was selected.")

    if name_column == name_none_label:
        name_column = None

    if not id_columns and name_column is None:
        raise SystemExit(
            "At least one of 'ID' or 'name' must be selected."
        )

    # Value columns: all remaining columns, optional and possibly multiple
    excluded = set(id_columns)
    if name_column is not None:
        excluded.add(name_column)
    remaining_for_values = [
        choice for choice in column_choices if choice.value not in excluded
    ]
    value_columns: list[str] = []
    value_renames: dict[str, str] = {}
    if remaining_for_values:
        value_columns = list(
            questionary.checkbox(
                PROMPT_SELECT_VALUE_COLUMNS,
                choices=remaining_for_values,
            ).ask()
            or []
        )
        if value_columns:
            value_renames = _prompt_value_renames(value_columns, id_columns, name_column)

    return id_columns, name_column, list(value_columns), value_renames


def narrow_to_single_column_step(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Reduce the DataFrame to the chosen ID/name/value columns."""
    # If a meta configuration from JSON is present and valid, prefer those column choices.
    selections = get_selections()
    meta = getattr(selections, "meta_config", None)

    if isinstance(meta, dict):
        id_column_meta = meta.get("id_column")
        id_columns_meta = meta.get("id_columns")
        name_column_meta = meta.get("name_column")
        value_columns_meta = meta.get("value_columns")
        if isinstance(value_columns_meta, str):
            value_columns_meta = [value_columns_meta]
        elif not isinstance(value_columns_meta, list):
            value_columns_meta = []

        columns = list(dataframe.columns)
        if isinstance(id_columns_meta, str):
            id_columns_meta = [id_columns_meta]
        elif not isinstance(id_columns_meta, list):
            id_columns_meta = []
        if not id_columns_meta and id_column_meta is not None:
            id_columns_meta = [id_column_meta]

        valid = True
        for col in id_columns_meta:
            if col not in columns:
                valid = False
                break
        if name_column_meta is not None and name_column_meta not in columns:
            valid = False
        if not id_columns_meta and name_column_meta is None:
            valid = False

        if valid:
            id_columns = list(id_columns_meta)
            name_column = name_column_meta
            value_columns = [
                c
                for c in value_columns_meta
                if c in columns and c not in set(id_columns + ([name_column] if name_column else []))
            ]
            value_renames = {c: c for c in value_columns}
        else:
            id_columns, name_column, value_columns, value_renames = _choose_columns(dataframe)
    else:
        id_columns, name_column, value_columns, value_renames = _choose_columns(dataframe)

    # Persist selections
    set_id_columns(id_columns)
    if name_column is not None:
        # Keep legacy 'column' in sync with the name column
        set_name_column(name_column)
    elif id_columns:
        # Fallback: if only an ID column is used, treat it as source column
        set_column(id_columns[0])

    renamed_value_columns = [value_renames.get(col, col) for col in value_columns]

    # Persist value columns (export only)
    set_value_columns(renamed_value_columns)

    # Keep only the selected columns (deduplicated)
    selected_cols: list[str] = []
    for col in (*id_columns, name_column, *renamed_value_columns):
        if col is not None and col not in selected_cols:
            selected_cols.append(col)

    # Work on a copy of the selected columns and strip leading/trailing
    # whitespace from string values in all selected columns.
    original_selection = [col for col in (*id_columns, name_column, *value_columns) if col is not None]
    result = dataframe[original_selection].copy()
    if value_renames:
        result = result.rename(columns=value_renames)
    for col in selected_cols:
        if result[col].dtype == "object":
            result[col] = result[col].map(
                lambda v: v.strip() if isinstance(v, str) else v
            )

    return result
