"""Text utilities used across the mapping pipeline."""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable
import pandas as pd
import math


def normalize_string(value: object) -> str:
    """Normalize a string for robust exact comparisons.

    Rules:
    - convert to lowercase via ``casefold`` (also maps ß→ss)
    - replace German umlauts (ä→ae, ö→oe, ü→ue)
    - remove diacritics from remaining letters (NFKD + drop Mn)
    - remove digits and replace any non [a-z] with a single space
    - collapse multiple spaces and trim
    """
    if value is None:
        return ""
    # Treat pandas/NumPy NA/NaN as empty
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        # Fallback for types without isna semantics
        if isinstance(value, float) and math.isnan(value):
            return ""
    text = str(value)
    text = text.casefold()
    text = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    # strip diacritics
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    # keep only letters a-z: remove digits and other chars → space
    text = re.sub(r"[^a-z]+", " ", text)
    # collapse spaces
    return " ".join(text.split())


def normalize_many(values: Iterable[object]) -> list[str]:
    """Normalize an iterable of values into a list of strings."""
    return [normalize_string(v) for v in values]


# def normalize_id(value: object) -> str:
#     """Normalize an ID value for robust comparisons.
#
#     Rules:
#     - treat pandas/NumPy NA/NaN as empty
#     - convert to string
#     - uppercase and strip surrounding whitespace
#     - remove spaces and punctuation, but keep all letters and digits
#
#     Examples:
#     - \"de 3\" -> \"DE3\"
#     - \"DE-03\" -> \"DE03\"
#     - \"01001\" -> \"01001\"
#     - \"NUTS3\" -> \"NUTS3\"
#     """
#     if value is None:
#         return ""
#     try:
#         if pd.isna(value):
#             return ""
#     except TypeError:
#         if isinstance(value, float) and math.isnan(value):
#             return ""
#     text = str(value).strip().upper()
#     # keep only letters and digits; drop separators/whitespace
#     text = re.sub(r"[^0-9A-Z]+", "", text)
#     return text


# def normalize_id_many(values: Iterable[object]) -> list[str]:
#     """Normalize an iterable of ID values into a list of strings."""
#     return [normalize_id(v) for v in values]
