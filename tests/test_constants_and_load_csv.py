from __future__ import annotations

from pathlib import Path

import pandas as pd

from geo_mapper.pipeline.constants import infer_dataset_family
from geo_mapper.pipeline.load_csv import _drop_completely_empty_rows


def test_infer_dataset_family_detects_lau_and_nuts() -> None:
    assert infer_dataset_family(Path("geo_mapper/geodata_clean/csv/LAU/2024/file.csv")) == "lau"
    assert infer_dataset_family(Path("geo_mapper/geodata_clean/csv/NUTS_3/2016/file.csv")) == "nuts"
    assert infer_dataset_family("NUTS_2/2020/data.csv") == "nuts"
    assert infer_dataset_family("some/other/path.csv") is None


def test_drop_completely_empty_rows_removes_only_fully_empty_rows() -> None:
    df = pd.DataFrame(
        {
            "a": ["", "  ", "x", None],
            "b": [None, "y", " ", "z"],
        }
    )

    cleaned = _drop_completely_empty_rows(df)

    # Row 0: a="", b=None  -> empty
    # Row 1: a="  ", b="y" -> not empty
    # Row 2: a="x", b=" "  -> not empty
    # Row 3: a=None, b="z" -> not empty
    assert list(cleaned.index) == [1, 2, 3]
    assert cleaned.loc[2, "a"] == "x"
