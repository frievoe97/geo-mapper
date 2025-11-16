"""Central place for shared constants used across the pipeline.

This module contains:
- filesystem/layout paths
- shared sentinel values
- mapper configuration (order / defaults / priorities)
- user-facing prompt texts
- shared geodata level metadata and mapping rules
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Final, Iterable


# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------

GEODATA_RAW_ROOT: Final[Path] = Path("geodata_raw")
GEODATA_CLEAN_ROOT: Final[Path] = Path("geodata_clean")

GEOJSON_ROOT: Final[Path] = GEODATA_CLEAN_ROOT / "geojson"
GEODATA_CSV_ROOT: Final[Path] = GEODATA_CLEAN_ROOT / "csv"

RESULTS_ROOT: Final[Path] = Path("results")


# ------------------------------------------------------------------------------
# Generic sentinels / choices
# ------------------------------------------------------------------------------

UNKNOWN_OPTION: Final[str] = "unknown"

# Supported geodata dataset families
DATASET_CHOICES: Final[tuple[str, ...]] = ("NUTS", "LAU")


# ------------------------------------------------------------------------------
# Geodata levels and titles
# ------------------------------------------------------------------------------

LEVEL_VALUES: Final[list[str]] = ["NUTS 0", "NUTS 1", "NUTS 2", "NUTS 3", "LAU"]

LEVEL_TITLES: Final[dict[str, str]] = {
    "NUTS 0": "NUTS 0 (Deutschland gesamt)",
    "NUTS 1": "NUTS 1 (Bundesländer)",
    "NUTS 2": "NUTS 2 (Regionen / Regierungsbezirke)",
    "NUTS 3": "NUTS 3 (Landkreise / kreisfreie Städte)",
    "LAU": "LAU (Gemeinden)",
}


# ------------------------------------------------------------------------------
# Mapper configuration and rules
# ------------------------------------------------------------------------------

DEFAULT_MAPPERS: Final[list[str]] = [
    "id_exact",
    "unique_name",
    "token_permutation",
    "regex_replace",
    "suffix_variants",
    "fuzzy_confident",
]


def _build_priority_mapping(order: Iterable[str]) -> dict[str, int]:
    return {name: idx for idx, name in enumerate(order)}


MAPPER_PRIORITY: Final[dict[str, int]] = _build_priority_mapping(DEFAULT_MAPPERS)


# Rules for the regex_replace mapper: list of (pattern, replacement)
REGEX_REPLACEMENTS: Final[list[tuple[str, str]]] = [
    (r"\bHanse- und Universitätsstadt\b", "Kreisfreie Stadt"),
    (r"\bLandeshauptstadt\b", "Kreisfreie Stadt"),
    (r"\bLandeshauptstadt\b", "Stadtkreis"),
    (r"\bdocumenta-Stadt\b", "Kreisfreie Stadt"),
    (r"\bHansestadt\b", "Kreisfreie Stadt"),
    (r"\bUniversitätsstadt\b", "Stadtkreis"),
    (r"\bWissenschaftsstadt\b", "Kreisfreie Stadt"),
    (r"\bStadt der FernUniversität\b", "Kreisfreie Stadt"),
    (r"\bFreie und Hansestadt\b", ""),
    (r"\bStadt\b", "Kreisfreie Stadt"),
    (r"\bWissenschaftsstadt\b", ""),
    (r"\bStadt\b", "Stadtkreis"),
    (r"\bStadt\b", ""),
    (r"\bLandkreis\b", ""),
    (r"\bBL\b", ""),
    (r"\bDE\b", ""),
    (r"\bKreis\b", ""),
    (r"\bOberpfalz\b", "Opf "),
    (r"\bin der\b", "i. d. "),
    (r"\bi\. d\.\b", "in der "),
    (r"\ban der\b", "a. d. "),
    (r"\ba\. d\.\b", "an der "),
    (r"\bam\b", "a. "),
    (r"\ba\.\b", "am "),
    (r"\bim\b", "i. "),
    (r"\bi\.\b", "im "),
    (r"\bLandkreis\b", "(D)"),
    (r"\bEifelkreis\b", ""),
    (r",\s*Stadt,\s+kreisfreie Stadt\b", ", Kreisfreie Stadt"),
    (r",\s*kreisfreie Stadt,\s+Hansestadt\b", ", Kreisfreie Stadt"),
    (r",\s*kreisfreie Stadt,\s+Landeshauptstadt\b", ", Kreisfreie Stadt"),
    (r",\s*kreisfreie Stadt,\s+documenta-Stadt\b", ", Kreisfreie Stadt"),
    (r",\s*Hansestadt,\s+kreisfreie Stadt\b", ", Kreisfreie Stadt"),
]

# Title words used to build variants in the suffix_variants mapper
SUFFIX_TITLE_WORDS: Final[list[str]] = [
    "Stadtkreis",
    "Landkreis",
    "Kreisfreie Stadt",
    "DE",
    "Eifelkreis",
    "Stadt",
]


# ------------------------------------------------------------------------------
# Fuzzy mapper thresholds and bonuses
# ------------------------------------------------------------------------------

FUZZY_MIN_BASE: Final[float] = 55.0
FUZZY_MIN_TOTAL: Final[float] = 64.0
FUZZY_MARGIN_MIN: Final[float] = 8.0
FUZZY_MARGIN_KFS_LK_NOHINT: Final[float] = 12.0
FUZZY_TYPE_BONUS: Final[float] = 10.0
FUZZY_STRUCT_BONUS: Final[float] = 6.0


# Regex patterns used by the fuzzy_confident mapper
CSV_DECOR_RE = re.compile(
    r"\b(kreisfreie\s+stadt|stadtkreis|landkreis|kreis)\b",
    re.IGNORECASE,
)
EXCEL_DECOR_RE = re.compile(
    r"\b(landeshauptstadt|documenta[-\s]?stadt|wissenschaftsstadt|klingenstadt|"
    r"freie\s+und\s+hansestadt|stadt(?:\s+der\s+fernuniversität)?)\b",
    re.IGNORECASE,
)
KFS_RE = re.compile(
    r"\b(kreisfreie\s+stadt|stadtkreis)\b",
    re.IGNORECASE,
)
LANDKREIS_RE = re.compile(
    r"\b(landkreis|kreis)\b",
    re.IGNORECASE,
)


# ------------------------------------------------------------------------------
# Questionary prompts / labels
# ------------------------------------------------------------------------------

# CSV / Excel loading
PROMPT_SELECT_WORKSHEET: Final[str] = "Select the worksheet (sheet) to load:"

# Column selection
PROMPT_SELECT_ID_COLUMN: Final[str] = "Which column contains the IDs?"
PROMPT_SELECT_NAME_COLUMN: Final[str] = "Which column contains the names?"
PROMPT_SELECT_VALUE_COLUMNS: Final[
    str
] = "Which columns contain values that should be included in the export? (optional)"

ID_NONE_LABEL: Final[str] = "<Do not use an ID column>"
NAME_NONE_LABEL: Final[str] = "<Do not use a name column>"

# Geodata selection
PROMPT_SELECT_GEODATA_LEVEL: Final[str] = "Which geodata level do you want to use?"
PROMPT_SELECT_GEODATA_VERSION: Final[
    str
] = "Choose the version (year) for {level}:"

# Mapping selection
PROMPT_SELECT_MAPPERS: Final[str] = "Which mapping steps should be executed?"

# Export selection
PROMPT_SELECT_EXPORT_SOURCE: Final[
    str
] = "Which geodata source should be used for manual mapping and export?"

# Manual mapping (dialog-based)
PROMPT_MANUAL_SELECT_INPUT: Final[
    str
] = "Which input value do you want to map manually?"
PROMPT_MANUAL_SEARCH_GEODATA: Final[
    str
] = "Optional: search term for geodata names (leave empty to list all):"
PROMPT_MANUAL_SELECT_GEODATA: Final[
    str
] = "Choose the matching geodata entry:"
LABEL_MANUAL_UNDO_LAST: Final[str] = "↩ Undo last manual mapping"
LABEL_MANUAL_DONE: Final[str] = "Done (no further manual mappings)"
LABEL_MANUAL_NO_MAPPING: Final[str] = "No mapping / back"


# Curses-based manual mapping
CURSES_MANUAL_HELP: Final[
    str
] = "TAB/←→: switch pane, ↑/↓: move, ENTER: map, u: undo, q: finish"
