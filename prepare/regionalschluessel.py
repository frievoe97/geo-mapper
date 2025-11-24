"""Regionalschlüssel-Ergänzungen aus unterschiedlichen Quellen."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def _clean_cell(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


@dataclass(frozen=True)
class RegionalschluesselSource:
    """Definition einer Quelle für Regionalschlüssel."""

    relative_path: str
    sheet_name: str
    usecols: str
    nuts_index: int
    region_index: int
    description: str
    header: int | list[int] | None = None
    skip_nuts_values: tuple[str, ...] = field(default_factory=tuple)

    def load_rows(self, base_dir: Path, log: logging.Logger) -> list[tuple[str, str]]:
        """Lese alle (NUTS, Regionalschlüssel)-Paare aus dieser Quelle."""
        path = base_dir / self.relative_path
        if not path.exists():
            log.warning("Regionalschlüsselquelle %s fehlt (%s)", self.description, path)
            return []
        try:
            df = pd.read_excel(
                path,
                sheet_name=self.sheet_name,
                usecols=self.usecols,
                header=self.header,
                dtype=str,
            )
        except Exception as exc:  # pylint: disable=broad-except
            log.error(
                "Fehler beim Lesen der Regionalschlüsselquelle %s (%s): %s",
                self.description,
                path,
                exc,
            )
            return []
        rows: list[tuple[str, str]] = []
        skip_upper = {s.upper() for s in self.skip_nuts_values}
        for raw in df.itertuples(index=False, name=None):
            try:
                nuts_val = raw[self.nuts_index]
                region_val = raw[self.region_index]
            except IndexError:
                continue
            nuts_id = _clean_cell(nuts_val)
            if not nuts_id:
                continue
            if nuts_id.upper() in skip_upper:
                continue
            region_code = _clean_cell(region_val)
            rows.append((nuts_id, region_code))
        return rows


REGION_SOURCES: tuple[RegionalschluesselSource, ...] = (
    RegionalschluesselSource(
        relative_path="ags_nuts/vgrdl_r2b2_bs2023_0.xlsx",
        sheet_name="1.1",
        usecols="B:C",
        nuts_index=0,
        region_index=1,
        description="VGRdL Tabelle 1.1",
        header=None,
        skip_nuts_values=("EU-CODE", "EU CODE", "EU-Code"),
    ),
    RegionalschluesselSource(
        relative_path="ags_nuts/04-kreise.xlsx",
        sheet_name="Kreisfreie Städte u. Landkreise",
        usecols="A:C",
        nuts_index=2,
        region_index=0,
        description="04-kreise.xlsx",
        header=None,
        skip_nuts_values=("NUTS3",),
    ),
)


def load_regionalschluessel_mapping(
    base_dir: Path, log: logging.Logger | None = None
) -> dict[str, str]:
    """Lade alle Regionalschlüssel aus den konfigurierten Quellen."""
    log = log or logger
    mapping: dict[str, str] = {}
    for source in REGION_SOURCES:
        rows = source.load_rows(base_dir, log)
        added = 0
        for nuts_id, region_code in rows:
            if nuts_id not in mapping:
                mapping[nuts_id] = region_code
                added += 1
        log.info(
            "%s liefert %d Einträge (%d neu)",
            source.description,
            len(rows),
            added,
        )
    log.info("Regionalschlüssel geladen: %d eindeutige Werte", len(mapping))
    return mapping


__all__ = ["load_regionalschluessel_mapping"]
