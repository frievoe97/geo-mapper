from __future__ import annotations

from pathlib import Path

import pandas as pd

from geo_mapper.pipeline.manual_mapping import _apply_meta_manual_mappings
from geo_mapper.pipeline.storage import set_geodata_frames, set_meta_config, set_export_geodata_source, set_id_columns, set_name_column


def test_apply_meta_manual_mappings_applies_entries_once() -> None:
    # Simple input DataFrame with a name column.
    dataframe = pd.DataFrame({"name": ["A", "B", "C"]})
    geodata_frame = pd.DataFrame({"id": ["G1", "G2"], "name": ["Geo1", "Geo2"]})
    source_path = "/tmp/NUTS_3/2024/file.csv"

    set_geodata_frames([(Path(source_path), geodata_frame)])
    mapping_df = pd.DataFrame(
        {
            "mapped_value": [pd.NA, pd.NA, pd.NA],
            "mapped_label": [pd.NA, pd.NA, pd.NA],
            "mapped_by": [pd.NA, pd.NA, pd.NA],
            "mapped_source": [pd.NA, pd.NA, pd.NA],
            "mapped_param": [pd.NA, pd.NA, pd.NA],
        }
    )
    set_export_geodata_source(source_path)
    set_id_columns([])
    set_name_column("name")

    set_meta_config(
        {
            "manual_mappings": [
                {
                    "input_ids": {},
                    "input_name": "B",
                    "geodata_id": "G2",
                    "geodata_name": "Geo2",
                }
            ]
        }
    )

    mapping_df = _apply_meta_manual_mappings(
        dataframe=dataframe,
        mapping_df=mapping_df,
        geodata_frame=geodata_frame,
        source_path=source_path,
    )

    # Only the row with name "B" is mapped, exactly once.
    assert mapping_df.loc[1, "mapped_value"] == "G2"
    assert mapping_df.loc[1, "mapped_label"] == "Geo2"
    assert mapping_df.loc[1, "mapped_by"] == "manual"
    assert mapping_df.loc[1, "mapped_source"] == source_path
    assert mapping_df["mapped_value"].notna().sum() == 1
