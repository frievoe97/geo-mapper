from __future__ import annotations

from pathlib import Path

import pandas as pd

from geo_mapper.pipeline.mapping.mappers.exact_id import (
    _normalize_id,
    _map_single_frame,
)
from geo_mapper.pipeline.mapping.mappers.unique_name import unique_name_mapper
from geo_mapper.pipeline.mapping.mappers.regex_replace import (
    regex_replace_mapper,
    set_used_ids_for_source as set_used_ids_for_regex,
)
from geo_mapper.pipeline.mapping.mappers.token_permutation import (
    token_permutation_mapper,
    set_used_ids_for_source as set_used_ids_for_token_perm,
)


def test_exact_id_normalize_id_behaviour() -> None:
    assert _normalize_id(None, strip_leading_zeroes=False) is None
    assert _normalize_id(float("nan"), strip_leading_zeroes=False) is None
    assert _normalize_id(1, strip_leading_zeroes=False) == "1"
    assert _normalize_id(1.0, strip_leading_zeroes=False) == "1"
    assert _normalize_id(1.5, strip_leading_zeroes=False) == "1.5"
    # Leading zeros only removed when strip_leading_zeroes=True
    assert _normalize_id("0012", strip_leading_zeroes=False) == "0012"
    assert _normalize_id("0012", strip_leading_zeroes=True) == "12"
    # Edge case: all zeros -> single "0"
    assert _normalize_id("0000", strip_leading_zeroes=True) == "0"


def test_exact_id_vs_id_without_leading_zero() -> None:
    # Input provides IDs without leading zeros, geodata has leading zeros.
    df = pd.DataFrame({"id": ["1", "10", "5"]})
    geo = pd.DataFrame(
        {
            "id": ["001", "010", "5"],
            "name": ["A", "B", "C"],
        }
    )
    csv_path = Path("/tmp/test.csv")

    # exact_id: compares 1:1 without stripping leading zeros.
    exact_out = _map_single_frame(
        df_slice=df,
        csv_path=csv_path,
        frame=geo,
        source_cols=["id"],
        strip_leading_zeroes=False,
    )
    # Only "5" finds a direct ID match.
    mv_exact = list(exact_out["mapped_value"])
    assert pd.isna(mv_exact[0])
    assert pd.isna(mv_exact[1])
    assert mv_exact[2] == "5"

    # id_without_leading_zero: strips leading zeros; all three match.
    nozero_out = _map_single_frame(
        df_slice=df,
        csv_path=csv_path,
        frame=geo,
        source_cols=["id"],
        strip_leading_zeroes=True,
    )
    assert list(nozero_out["mapped_value"]) == ["1", "10", "5"]


def test_unique_name_mapper_uses_normalized_names() -> None:
    df = pd.DataFrame({"name": ["München", "   muenchen  ", "Berlin"]})
    geo = pd.DataFrame({"id": ["A", "B"], "name": ["Muenchen", "Berlin"]})
    frames = [(Path("/tmp/geo.csv"), geo)]

    out = unique_name_mapper(df_slice=df, geodata_frames=frames, source_col="name")

    # Both variants of München should map to the same ID.
    assert out.loc[0, "mapped_value"] == "A"
    assert out.loc[1, "mapped_value"] == "A"
    # Berlin matches uniquely.
    assert out.loc[2, "mapped_value"] == "B"


def test_regex_replace_mapper_uses_replacements_and_respects_used_ids() -> None:
    df = pd.DataFrame({"name": ["Hansestadt Hamburg", "Hansestadt Lübeck"]})
    geo = pd.DataFrame(
        {
            "id": ["1", "2"],
            "name": ["Hamburg", "Lübeck"],
        }
    )
    csv_path = Path("/tmp/geo.csv")
    frames = [(csv_path, geo)]

    # First run with no used IDs: both cities should be mapped.
    set_used_ids_for_regex(str(csv_path), set())
    out1 = regex_replace_mapper(df_slice=df, geodata_frames=frames, source_col="name")
    assert set(out1["mapped_value"]) == {"1", "2"}

    # If ID "1" was already used, only "2" may be mapped.
    set_used_ids_for_regex(str(csv_path), {"1"})
    out2 = regex_replace_mapper(df_slice=df, geodata_frames=frames, source_col="name")
    assert out2.loc[0, "mapped_value"] is pd.NA or pd.isna(out2.loc[0, "mapped_value"])
    assert out2.loc[1, "mapped_value"] == "2"


def test_token_permutation_mapper_matches_with_suffixes_and_used_ids() -> None:
    df = pd.DataFrame({"name": ["Rostock", "Rostock"]})
    # Geodata contains exactly one variant with a suffix like "Kreisfreie Stadt".
    geo = pd.DataFrame(
        {
            "id": ["1"],
            "name": ["Rostock Kreisfreie Stadt"],
        }
    )
    csv_path = Path("/tmp/geo.csv")
    frames = [(csv_path, geo)]

    # First run with no used IDs → both rows can map to the same ID.
    set_used_ids_for_token_perm(str(csv_path), set())
    out1 = token_permutation_mapper(df_slice=df, geodata_frames=frames, source_col="name")
    first_id = out1.loc[0, "mapped_value"]
    second_id = out1.loc[1, "mapped_value"]
    assert first_id == "1"
    assert second_id == "1"

    # Mark this ID as already used → the second row should receive no further match.
    set_used_ids_for_token_perm(str(csv_path), {str(first_id)})
    out2 = token_permutation_mapper(
        df_slice=df.iloc[[1]], geodata_frames=frames, source_col="name"
    )
    # Because ID "1" is already used, no further hit is allowed now.
    assert pd.isna(out2.loc[df.index[1], "mapped_value"])
