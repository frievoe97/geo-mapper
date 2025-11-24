"""Central place for shared constants used across the pipeline.

This module contains:
- filesystem/layout paths
- shared sentinel values
- mapper configuration (order / defaults / priorities)
- user-facing prompt texts
- shared geodata level metadata and mapping rules
"""

from __future__ import annotations

from pathlib import Path
from typing import Final, Iterable, Optional


# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------

PACKAGE_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
GEODATA_RAW_ROOT: Final[Path] = PACKAGE_ROOT / "geodata_raw"
GEODATA_CLEAN_ROOT: Final[Path] = PACKAGE_ROOT / "geodata_clean"

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
# Geodata ID column definitions
# ------------------------------------------------------------------------------

# Mapping of geodata dataset families to the ID columns that should be exported.
# Each entry is a list of (source_column_name, export_column_name) tuples.
GEODATA_ID_COLUMNS: Final[dict[str, list[tuple[str, str]]]] = {
    "lau": [
        ("id", "geodaten_id"),
    ],
    "nuts": [
        ("id_nuts", "geodaten_id_nuts"),
        ("id_ars", "geodaten_id_ars"),
    ],
}


def infer_dataset_family(csv_path: Path | str) -> Optional[str]:
    """Infer the dataset family (nuts|lau) from a geodata CSV path."""
    if isinstance(csv_path, Path):
        parts = csv_path.parts
    else:
        parts = Path(csv_path).parts
    lowered = [part.lower() for part in parts]
    if "lau" in lowered:
        return "lau"
    for part in lowered:
        if part.startswith("nuts"):
            return "nuts"
    return None


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
    "exact_id",
    "id_without_leading_zero",
    "unique_name",
    "regex_replace",
    "token_permutation",
]


def _build_priority_mapping(order: Iterable[str]) -> dict[str, int]:
    return {name: idx for idx, name in enumerate(order)}


MAPPER_PRIORITY: Final[dict[str, int]] = _build_priority_mapping(DEFAULT_MAPPERS)


# Rules for the regex_replace mapper: list of (pattern, replacement)
REGEX_REPLACEMENTS: Final[list[tuple[str, str]]] = [
    (r"\ba\.\s*", "am "),
    (r"\bam\b", "a."),
    (r"\ban der\b", "a.d."),
    (r"\bBL\b", ""),
    (r"\bdocumenta-Stadt\b", ""),
    (r"\bEifelkreis\b", ""),
    (r"\bHansestadt\b", "Kreisfreie Stadt"),
    (r"\bim\b", "i."),
    (r"\bin der Oberpfalz\b", "i. d. Opf"),
    (r"\bkreisfreie Stadt\b", ""),
    (r"\bkreisfreie Stadt\b", "Stadtkreis"),
    (r"\bKreis\b", ""),
    (r"\bLandeshauptstadt\b", ""),
    (r"\bLandeshauptstadt\b", "Stadtkreis"),
    (r"\bLandkreis\b", ""),
    (r"\bLandkreis\b", "(DE)"),
    (r"\bSt\.", "Kreisfreie Stadt"),
    (r"\bStadt\b", "Kreisfreie Stadt"),
    (r"\bStadt\b", "Stadtkreis"),
    (r"\bStadt\b", ""),
    (r"\bUniversitätsstadt\b", "Stadtkreis"),
    (r"\bWissenschaftsstadt\b", ""),
    (r"\bWissenschaftsstadt\b", "Kreisfreie Stadt"),
    (r"\bLandeshauptstadt\b", "Kreisfreie Stadt"),
    (r"\bdocumenta-Stadt\b", "Kreisfreie Stadt"),
    (r"\bkr\.f\. St\.", "Kreisfreie Stadt"),
    (r"\bkreisfr\.\s*Stadt\b", "Kreisfreie Stadt"),
    (r"\bSalzlandkreis\b", "Salzland"),
    (r"\bBurgenlandkreis\b", "Burgenland (D)"),
    (r"\bSächs\.", "Sächsische"),
    (r"\bRegionalverband\b", "Stadtverband"),
    (r"\bZwickau\b", "Zwichau"), # NUTS 2010 Level 3 Fehlschreibung
(r"\bStadt der FernUniversität\b", "Kreisfreie Stadt"),
(r"\bKlingenstadt\b", "Kreisfreie Stadt"),
(r"\bFreie und Hansestadt\b", ""),
(r"\bUniversitätsstadt\b", ""),
(r"(?<=\s)Kreis\b", ""),
(r"\bHansestadt\b", ""),



]

# Title words used to build variants in the token_permutation mapper
SUFFIX_TITLE_WORDS: Final[list[str]] = [
    "Stadtkreis",
    "Landkreis",
    "Kreisfreie Stadt",
    "DE",
    "D",
    "Eifelkreis",
    # "Stadt",
    "Kreis",
    # "Landeshauptstadt",
    # "documenta-Stadt",
    # "Universitätsstadt",
    # "Wissenschaftsstadt",
    # "Hansestadt"
]


# ------------------------------------------------------------------------------
# Questionary prompts / labels
# ------------------------------------------------------------------------------

# CSV / Excel loading
PROMPT_SELECT_WORKSHEET: Final[str] = "Select the worksheet (sheet) to load:"

# Column selection
PROMPT_SELECT_ID_COLUMN: Final[str] = "Which column(s) contain the IDs?"
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
