"""Mapper: Suffix anhängen, normalisieren und Tokens sortieren.

Ablauf (pro Quellwert):
- für jedes Suffix in ``SUFFIX_TITLE_WORDS``:
  - baue Variante: ``<Input> + " " + <Suffix>``
  - normalisiere den String
  - splitte an Leerzeichen in Tokens, sortiere die Tokens alphabetisch
    und füge sie wieder zu einem Key zusammen
- für alle Geodaten‑Namen wird derselbe normalisierte, token‑sortierte Key
  gebildet
- es wird gemappt, wenn über alle Varianten genau EIN eindeutiger Treffer
  (eine Geodaten‑ID) existiert
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple, cast

import pandas as pd

from ...constants import SUFFIX_TITLE_WORDS
from ...utils.text import normalize_string


def _token_key(text: str) -> str:
    """Normalisiere Text, sortiere Tokens alphabetisch und bilde Key."""

    norm = normalize_string(text)
    if not norm:
        return ""
    tokens = norm.split()
    tokens.sort()
    return " ".join(tokens)


def _build_geodata_lookup(frame: pd.DataFrame) -> Dict[str, List[Tuple[str, str]]]:
    """Baue Lookup: normalisierter, token‑sortierter Name -> Liste von (id, original_name)."""

    lookup: Dict[str, List[Tuple[str, str]]] = {}
    for raw, gid in zip(frame.get("name", []), frame.get("id", []), strict=False):
        key = _token_key(str(raw))
        if not key:
            continue
        lookup.setdefault(key, []).append((str(gid), str(raw)))
    return lookup


# Optional: bereits verwendete Geodaten-IDs pro CSV, wird vom Orchestrator gesetzt.
USED_IDS_BY_SOURCE: Dict[str, set[str]] = {}


def set_used_ids_for_source(source: str, ids: set) -> None:
    """Bereits verwendete Geodaten-IDs für eine gegebene CSV merken."""

    USED_IDS_BY_SOURCE[source] = {str(gid) for gid in ids}


def token_permutation_mapper(
    df_slice: pd.DataFrame, geodata_frames: List[Tuple[Path, pd.DataFrame]], source_col: str
) -> pd.DataFrame:
    """Mapper, der Suffixe anhängt, normalisiert und Tokens sortiert.

    Für jeden Eingabewert:
    - für jedes Suffix in ``SUFFIX_TITLE_WORDS`` wird eine Variante
      ``"<Input> " + Suffix`` gebildet
    - jede Variante wird normalisiert, in Tokens gesplittet, Tokens werden
      alphabetisch sortiert und wieder zusammengefügt (Key)
    - für die Geodaten wird derselbe Key pro Name gebildet
    - wenn über alle Varianten genau eine eindeutige Geodaten‑ID gefunden wird
      (unter Berücksichtigung bereits genutzter IDs), wird gemappt
    """

    if not geodata_frames:
        return pd.DataFrame(
            index=df_slice.index,
            columns=["mapped_by", "mapped_value", "mapped_source", "mapped_label", "mapped_param"],
        ).assign(
            mapped_by=cast(Any, pd.NA),
            mapped_value=cast(Any, pd.NA),
            mapped_source=cast(Any, pd.NA),
            mapped_label=cast(Any, pd.NA),
            mapped_param=cast(Any, pd.NA),
        )

    csv_path, frame = geodata_frames[0]
    if not {"name", "id"}.issubset(frame.columns):
        return pd.DataFrame(
            index=df_slice.index,
            columns=["mapped_by", "mapped_value", "mapped_source", "mapped_label", "mapped_param"],
        ).assign(
            mapped_by=cast(Any, pd.NA),
            mapped_value=cast(Any, pd.NA),
            mapped_source=cast(Any, pd.NA),
            mapped_label=cast(Any, pd.NA),
            mapped_param=cast(Any, pd.NA),
        )

    lookup = _build_geodata_lookup(frame)
    used_ids = USED_IDS_BY_SOURCE.get(str(csv_path), set())

    out_rows = {
        "mapped_by": [],
        "mapped_value": [],
        "mapped_source": [],
        "mapped_label": [],
        "mapped_param": [],
    }

    for i in df_slice.index:
        original = str(df_slice.at[i, source_col])

        # Varianten: Original selbst + Original + " " + Suffix (für jedes Suffix)
        variants: List[str] = [original]
        for suffix in SUFFIX_TITLE_WORDS:
            variants.append(f"{original} {suffix}")

        # Bilde pro Variante den normalisierten, token‑sortierten Key
        # und sammle eindeutige Kandidaten (nach Filtern bereits benutzter IDs).
        # Wir merken uns dabei den Key selbst, damit er später in mapped_param
        # (parameter-Spalte) gespeichert werden kann.
        hits: List[Tuple[str, str, str]] = []  # (normalized_key, geodata_id, geodata_label)
        for variant in variants:
            key = _token_key(variant)
            if not key:
                continue
            candidates = lookup.get(key, [])
            # IDs, die in früheren Schritten bereits verwendet wurden,
            # werden ignoriert, damit z.B. nach Zuordnung der kreisfreien
            # Stadt die Landkreis-Variante noch eindeutig zugeordnet
            # werden kann.
            available = [(gid, label) for gid, label in candidates if str(gid) not in used_ids]
            if len(available) == 1:
                gid, label = available[0]
                hits.append((key, gid, label))

        # Eindeutig nur dann, wenn alle Treffer auf dieselbe ID zeigen.
        if hits:
            unique_ids = {gid for _key, gid, _label in hits}
            if len(unique_ids) == 1:
                used_key, hit_id, hit_label = hits[0]
            else:
                used_key = hit_id = hit_label = None
        else:
            used_key = hit_id = hit_label = None

        if hit_id is not None:
            out_rows["mapped_by"].append("token_permutation")
            out_rows["mapped_value"].append(hit_id)
            out_rows["mapped_source"].append(str(csv_path))
            out_rows["mapped_label"].append(hit_label)
            # mapped_param: welcher normalisierte Token-Key gematcht hat
            out_rows["mapped_param"].append(used_key)
        else:
            out_rows["mapped_by"].append(pd.NA)
            out_rows["mapped_value"].append(pd.NA)
            out_rows["mapped_source"].append(pd.NA)
            out_rows["mapped_label"].append(pd.NA)
            out_rows["mapped_param"].append(pd.NA)

    return pd.DataFrame(out_rows, index=df_slice.index)
