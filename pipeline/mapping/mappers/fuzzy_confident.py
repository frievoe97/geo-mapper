"""Mapper: conservative fuzzy matching with heuristic bonuses and margin.

This approximates a prior project's matcher_04_ml_confident using difflib
similarity and simple type/decorator handling. It works per single CSV.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple
import logging

import difflib
import pandas as pd

from ...utils.text import normalize_string
from ...constants import (
    CSV_DECOR_RE,
    EXCEL_DECOR_RE,
    KFS_RE,
    LANDKREIS_RE,
    FUZZY_MIN_BASE,
    FUZZY_MIN_TOTAL,
    FUZZY_MARGIN_MIN,
    FUZZY_MARGIN_KFS_LK_NOHINT,
    FUZZY_TYPE_BONUS,
    FUZZY_STRUCT_BONUS,
)


logger = logging.getLogger(__name__)


def _type_pref(name_raw: str) -> str | None:
    if LANDKREIS_RE.search(name_raw):
        return "lk"
    if KFS_RE.search(name_raw) or EXCEL_DECOR_RE.search(name_raw):
        return "kfs"
    return None


def _cand_type(name_raw: str) -> str | None:
    if KFS_RE.search(name_raw):
        return "kfs"
    if LANDKREIS_RE.search(name_raw):
        return "lk"
    return None


def _clean_excel(raw: str) -> str:
    return EXCEL_DECOR_RE.sub("", str(raw)).strip()


def _clean_csv(raw: str) -> str:
    return CSV_DECOR_RE.sub("", str(raw)).strip()


def _sim(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio() * 100.0


def _score_pair(x_raw: str, x_norm: str, y_raw: str, y_norm: str) -> float:
    x_raw_clean = _clean_excel(x_raw)
    y_raw_clean = _clean_csv(y_raw)
    x_norm_clean = normalize_string(x_raw_clean)
    y_norm_clean = normalize_string(y_raw_clean)
    return max(_sim(x_norm, y_norm), _sim(x_norm_clean, y_norm_clean))


def fuzzy_confident_mapper(
    df_slice: pd.DataFrame, geodata_frames: List[Tuple[Path, pd.DataFrame]], source_col: str
) -> pd.DataFrame:
    if not geodata_frames:
        # No geodata available: return an empty-shaped result DataFrame.
        return pd.DataFrame(
            {
                "mapped_by": pd.Series(dtype="object", index=df_slice.index),
                "mapped_value": pd.Series(dtype="object", index=df_slice.index),
                "mapped_source": pd.Series(dtype="object", index=df_slice.index),
                "mapped_label": pd.Series(dtype="object", index=df_slice.index),
            }
        )
    csv_path, frame = geodata_frames[0]
    if not {"name", "id"}.issubset(frame.columns):
        # Geodata has no usable columns: return an empty-shaped result DataFrame.
        return pd.DataFrame(
            {
                "mapped_by": pd.Series(dtype="object", index=df_slice.index),
                "mapped_value": pd.Series(dtype="object", index=df_slice.index),
                "mapped_source": pd.Series(dtype="object", index=df_slice.index),
                "mapped_label": pd.Series(dtype="object", index=df_slice.index),
            }
        )

    # Prepare candidates view
    cand = frame[["id", "name"]].copy()
    cand["_raw"] = cand["name"].astype(str)
    cand["_norm"] = cand["_raw"].map(normalize_string)
    # "base" = norm without decor words
    cand["_base"] = cand["_norm"].map(lambda s: normalize_string(_clean_csv(s)))

    out_rows = {
        "mapped_by": [],
        "mapped_value": [],
        "mapped_source": [],
        "mapped_label": [],
        "mapped_param": [],
    }

    for i in df_slice.index:
        x_raw = str(df_slice.at[i, source_col])
        x_norm = normalize_string(x_raw)
        x_base = normalize_string(_clean_excel(x_raw))

        # Candidate subset: same base or prefix/suffix overlaps on normalized
        sub = cand[(cand["_base"] == x_base) | cand["_norm"].str.startswith(x_norm) | cand["_norm"].str.endswith(x_norm)]
        if sub.empty:
            out_rows["mapped_by"].append(pd.NA)
            out_rows["mapped_value"].append(pd.NA)
            out_rows["mapped_source"].append(pd.NA)
            out_rows["mapped_label"].append(pd.NA)
            out_rows["mapped_param"].append(pd.NA)
            continue

        x_pref = _type_pref(x_raw)

        scored: List[Tuple[float, float, str, str, str | None]] = []
        for _, r in sub.iterrows():
            y_raw = str(r["_raw"])
            y_norm = str(r["_norm"])
            base_score = _score_pair(x_raw, x_norm, y_raw, y_norm)
            y_type = _cand_type(y_raw)
            type_bonus = FUZZY_TYPE_BONUS if (x_pref and y_type == x_pref) else 0.0
            struct_bonus = FUZZY_STRUCT_BONUS if (r["_base"] == x_base) else 0.0
            total = base_score + type_bonus + struct_bonus
            scored.append((total, base_score, str(r["id"]), str(r["name"]), y_type))

        if not scored:
            out_rows["mapped_by"].append(pd.NA)
            out_rows["mapped_value"].append(pd.NA)
            out_rows["mapped_source"].append(pd.NA)
            out_rows["mapped_label"].append(pd.NA)
            out_rows["mapped_param"].append(pd.NA)
            continue

        scored.sort(key=lambda t: t[0], reverse=True)
        top_total, top_base, top_id, top_name, top_type = scored[0]
        if len(scored) == 1:
            if (top_base >= FUZZY_MIN_BASE) and (top_total >= FUZZY_MIN_TOTAL):
                out_rows["mapped_by"].append("fuzzy_confident")
                out_rows["mapped_value"].append(top_id)
                out_rows["mapped_source"].append(str(csv_path))
                out_rows["mapped_label"].append(top_name)
                out_rows["mapped_param"].append(top_total)
            else:
                out_rows["mapped_by"].append(pd.NA)
                out_rows["mapped_value"].append(pd.NA)
                out_rows["mapped_source"].append(pd.NA)
                out_rows["mapped_label"].append(pd.NA)
                out_rows["mapped_param"].append(pd.NA)
            continue

        second_total = scored[1][0]
        margin = top_total - second_total
        top_types = {t for *_rest, t in scored[: min(4, len(scored))]}
        mixed_types_no_hint = (x_pref is None) and ("kfs" in top_types) and ("lk" in top_types)
        margin_needed = max(
            FUZZY_MARGIN_MIN,
            FUZZY_MARGIN_KFS_LK_NOHINT if mixed_types_no_hint else FUZZY_MARGIN_MIN,
        )

        # hard guard: if explicit x_pref conflicts with top_type, skip
        if (x_pref is not None) and (top_type is not None) and (x_pref != top_type):
            out_rows["mapped_by"].append(pd.NA)
            out_rows["mapped_value"].append(pd.NA)
            out_rows["mapped_source"].append(pd.NA)
            out_rows["mapped_label"].append(pd.NA)
            out_rows["mapped_param"].append(pd.NA)
            continue

        if (top_base >= FUZZY_MIN_BASE) and (top_total >= FUZZY_MIN_TOTAL) and (margin >= margin_needed):
            out_rows["mapped_by"].append("fuzzy_confident")
            out_rows["mapped_value"].append(top_id)
            out_rows["mapped_source"].append(str(csv_path))
            out_rows["mapped_label"].append(top_name)
            out_rows["mapped_param"].append(top_total)
        else:
            out_rows["mapped_by"].append(pd.NA)
            out_rows["mapped_value"].append(pd.NA)
            out_rows["mapped_source"].append(pd.NA)
            out_rows["mapped_label"].append(pd.NA)
            out_rows["mapped_param"].append(pd.NA)

    return pd.DataFrame(out_rows, index=df_slice.index)
