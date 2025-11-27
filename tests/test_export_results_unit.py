from __future__ import annotations

from pathlib import Path

import pandas as pd

from geo_mapper.pipeline import export_results
from geo_mapper.pipeline.export_results import _stringify_geodata_id_value, _collect_geodata_id_values


def test_stringify_geodata_id_value_normalizes_ids() -> None:
    assert _stringify_geodata_id_value(None) is None
    assert _stringify_geodata_id_value(float("nan")) is None
    assert _stringify_geodata_id_value(1) == "1"
    assert _stringify_geodata_id_value(1.0) == "1"
    assert _stringify_geodata_id_value(1.5) == "1.5"
    # Nicht-leere Strings werden unverändert zurückgegeben (ohne Trim).
    assert _stringify_geodata_id_value("  ") == "  "
    assert _stringify_geodata_id_value("ABC") == "ABC"


def test_collect_geodata_id_values_uses_geodata_id_columns() -> None:
    frame = pd.DataFrame(
        {
            "id": [1, 2],
            "id_nuts": ["DE001", "DE002"],
            "id_ars": ["01001", "01002"],
            "name": ["A", "B"],
        }
    )
    source = "/tmp/NUTS_3/2024/file.csv"
    frame_by_source = {source: frame}
    dataset_by_source = {source: "nuts"}
    lookup_by_source = {source: {str(i): i - 1 for i in frame["id"]}}

    values = _collect_geodata_id_values(
        source_path=source,
        geodata_id=1,
        frame_by_source=frame_by_source,
        dataset_by_source=dataset_by_source,
        lookup_by_source=lookup_by_source,
    )

    # Für "nuts" werden id_nuts/id_ars als geodata_id_nuts/_ars exportiert.
    assert values == {"geodata_id_nuts": "DE001", "geodata_id_ars": "01001"}
