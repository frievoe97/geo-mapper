"""Pipeline step that determines which geodata dataset to reference (interactive)."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, List

import pandas as pd
import questionary

from ..storage import (
    set_geodata_type,
    set_geodata_version,
    set_nuts_level,
    get_selections,
)
from ..constants import (
    GEODATA_CSV_ROOT,
    UNKNOWN_OPTION,
    DATASET_CHOICES,
    LEVEL_TITLES,
    LEVEL_VALUES,
    PROMPT_SELECT_GEODATA_LEVEL,
    PROMPT_SELECT_GEODATA_VERSION,
)


def _dataset_dir_for_level(level_choice: str) -> Optional[Path]:
    """Return the dataset directory in geodata_clean/csv for a given level label."""
    if level_choice == UNKNOWN_OPTION:
        return None
    if level_choice == "LAU":
        return GEODATA_CSV_ROOT / "LAU"
    if level_choice.startswith("NUTS "):
        try:
            _, lvl = level_choice.split(" ", 1)
        except ValueError:
            return None
        return GEODATA_CSV_ROOT / f"NUTS_{lvl}"
    return None


def _available_versions(level_choice: str) -> List[str]:
    """List all version (year) directories for the requested level."""
    directory = _dataset_dir_for_level(level_choice)
    if not directory or not directory.exists():
        return []
    return sorted(entry.name for entry in directory.iterdir() if entry.is_dir() and entry.name)


def _prompt_select(message: str, choices: list[str], default: str) -> str:
    """Ask the user to select one of the provided choices."""
    selection = questionary.select(message, choices=choices, default=default).ask()
    if not selection:
        raise SystemExit(f"No choice made for '{message}'.")
    return selection


def _with_unknown(choices: list[str]) -> list[str]:
    """Prepend the 'unknown' option and avoid duplicates."""
    seen = {UNKNOWN_OPTION}
    result = [UNKNOWN_OPTION]
    for choice in choices:
        if choice not in seen and choice:
            seen.add(choice)
            result.append(choice)
    return result


def select_geodata_step(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Prompt the user for the geodata subset they want to use.

    Logic:
    1. Choose an administrative level (NUTS 0–3 or LAU).
    2. Choose the year/version for this level (derived from available directories).
    """
    # First check whether a meta configuration from JSON already specifies a valid level and year.
    selections = get_selections()
    meta = getattr(selections, "meta_config", None)
    if isinstance(meta, dict):
        # Support both new keys (geodata_level/geodata_year)
        # and legacy keys (level/year)
        meta_level = meta.get("geodata_level", meta.get("level"))
        meta_year = meta.get("geodata_year", meta.get("year"))
        if isinstance(meta_level, str):
            level_label = meta_level
            upper = level_label.upper().replace("_", " ").strip()
            level_choice_from_meta: Optional[str] = None
            if upper == "LAU":
                level_choice_from_meta = "LAU"
            elif upper.startswith("NUTS"):
                # Accept "NUTS 3", "NUTS3" or similar
                rest = upper.replace("NUTS", "").strip()
                if rest.isdigit():
                    level_choice_from_meta = f"NUTS {rest}"
            if level_choice_from_meta is not None:
                dataset_dir = _dataset_dir_for_level(level_choice_from_meta)
                if dataset_dir and dataset_dir.exists():
                    # Take the level from meta configuration (if it points to an existing directory)
                    if level_choice_from_meta == "LAU":
                        geodata_type = "LAU"
                        nuts_level = UNKNOWN_OPTION
                    else:
                        geodata_type = "NUTS"
                        _, lvl = level_choice_from_meta.split(" ", 1)
                        nuts_level = lvl
                    set_geodata_type(geodata_type)
                    set_nuts_level(nuts_level)

                    # If a valid year is given, use it and skip interactive prompts
                    if isinstance(meta_year, (str, int)):
                        year_str = str(meta_year)
                        if (dataset_dir / year_str).is_dir():
                            set_geodata_version(year_str)
                            return dataframe

                    # No or invalid year: level is fixed, year will be selected interactively.
                    version_choices_raw = _available_versions(level_choice_from_meta)
                    version_choices = _with_unknown(version_choices_raw)
                    version_default = (
                        version_choices[1] if len(version_choices) > 1 else UNKNOWN_OPTION
                    )
                    version_choice = _prompt_select(
                        PROMPT_SELECT_GEODATA_VERSION.format(level=level_choice_from_meta),
                        version_choices,
                        version_default,
                    )
                    set_geodata_version(version_choice)
                    return dataframe

    # Interactive selection of the level – with human-readable German descriptions
    try:
        from questionary import Choice

        level_titles = LEVEL_TITLES
        level_values: List[str] = list(LEVEL_VALUES)
        level_choices = [Choice(title=UNKNOWN_OPTION, value=UNKNOWN_OPTION)]
        for value in level_values:
            title = level_titles.get(value, value)
            level_choices.append(Choice(title=title, value=value))
        # Default by internal value, not by label
        default_value = "NUTS 3"
        level_choice = questionary.select(
            PROMPT_SELECT_GEODATA_LEVEL,
            choices=level_choices,
            default=default_value,
        ).ask()
    except Exception:
        # Fallback without Choice helper: use descriptive labels and map back to internal values
        level_titles = LEVEL_TITLES
        level_values_fallback: List[str] = list(LEVEL_VALUES)
        # Build display labels including the German description
        level_labels = [level_titles[v] for v in level_values_fallback]
        level_choices_fb = _with_unknown(level_labels)
        default_label = level_titles.get("NUTS 3", "NUTS 3")
        level_choice = _prompt_select(
            PROMPT_SELECT_GEODATA_LEVEL,
            level_choices_fb,
            default_label,
        )

    # Map UI labels back to internal level values if needed
    label_to_value = {
        "NUTS 0 (Deutschland gesamt)": "NUTS 0",
        "NUTS 1 (Bundesländer)": "NUTS 1",
        "NUTS 2 (Regionen / Regierungsbezirke)": "NUTS 2",
        "NUTS 3 (Landkreise / kreisfreie Städte)": "NUTS 3",
        "LAU (Gemeinden)": "LAU",
    }
    if level_choice in label_to_value:
        level_choice = label_to_value[level_choice]

    if level_choice == UNKNOWN_OPTION:
        # No restriction: allow both NUTS and LAU and all levels/years
        set_geodata_type(UNKNOWN_OPTION)
        set_nuts_level(UNKNOWN_OPTION)
        set_geodata_version(UNKNOWN_OPTION)
        return dataframe

    if level_choice == "LAU":
        geodata_type = "LAU"
        nuts_level = UNKNOWN_OPTION
    else:
        geodata_type = "NUTS"
        # Expected format: "NUTS X"
        try:
            _, lvl = level_choice.split(" ", 1)
        except ValueError:
            lvl = UNKNOWN_OPTION
        nuts_level = lvl

    set_geodata_type(geodata_type)
    set_nuts_level(nuts_level)

    version_choices_raw = _available_versions(level_choice)
    version_choices = _with_unknown(version_choices_raw)
    # Default to the newest year (if any), otherwise "unknown"
    version_default = (
        version_choices[1] if len(version_choices) > 1 else UNKNOWN_OPTION
    )
    version_choice = _prompt_select(
        PROMPT_SELECT_GEODATA_VERSION.format(level=level_choice),
        version_choices,
        version_default,
    )
    set_geodata_version(version_choice)

    return dataframe
