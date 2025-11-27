from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest


ROOT_DIR = Path(__file__).resolve().parent.parent
TEST_FILES_ROOT = ROOT_DIR / "test_files"


def _discover_cases() -> list[tuple[str, Path, Path, Path]]:
    """Return (case_id, input_file, meta_file, expected_results_dir) for all cases."""
    cases: list[tuple[str, Path, Path, Path]] = []
    if not TEST_FILES_ROOT.is_dir():
        return cases

    for test_dir in sorted(TEST_FILES_ROOT.glob("test_*")):
        if not test_dir.is_dir():
            continue
        for variant_dir in sorted(test_dir.iterdir()):
            if not variant_dir.is_dir():
                continue
            meta_file = variant_dir / "meta.yaml"
            if not meta_file.is_file():
                continue

            input_files = [
                p
                for p in variant_dir.iterdir()
                if p.is_file()
                and p.name != "meta.yaml"
                and not p.name.startswith("results_")
                and p.suffix.lower() in {".csv", ".xlsx", ".xlsm", ".xls"}
            ]
            if not input_files:
                continue
            # Jede Test-Variante sollte genau eine Eingabedatei haben.
            assert (
                len(input_files) == 1
            ), f"Expected exactly one input file in {variant_dir}, found: {input_files}"
            input_file = input_files[0]

            result_dirs = [
                p for p in variant_dir.iterdir() if p.is_dir() and p.name.startswith("results_")
            ]
            assert (
                len(result_dirs) == 1
            ), f"Expected exactly one results_* directory in {variant_dir}, found: {result_dirs}"
            expected_results_dir = result_dirs[0]

            case_id = f"{test_dir.name}/{variant_dir.name}"
            cases.append((case_id, input_file, meta_file, expected_results_dir))
    return cases


CASES = _discover_cases()


pytestmark = pytest.mark.filterwarnings(
    "ignore:Workbook contains no default style, apply openpyxl's default:UserWarning"
)


@pytest.mark.parametrize(
    "case_id,input_file,meta_file,expected_results_dir",
    CASES,
)
def test_cli_outputs_match_expected_results(
    case_id: str,
    input_file: Path,
    meta_file: Path,
    expected_results_dir: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run the CLI for each input/meta pair and compare CSV results."""
    # CLI: --data <input> --yaml <meta> --auto-export-source true
    # Results should be identical to the precomputed CSVs.
    from geo_mapper import cli
    from geo_mapper.pipeline import manual_mapping as mm  # type: ignore[attr-defined]

    # Patch: disable interactive manual-mapping UIs, but keep meta-based mappings.
    def _no_ui(
        dataframe,  # pd.DataFrame
        mapping_df,  # pd.DataFrame
        geodata_frame,
        source_path: str,
        source_col: str,
    ):
        return mapping_df

    monkeypatch.setattr(mm, "_run_curses_manual_mapping", _no_ui)
    monkeypatch.setattr(mm, "_run_questionary_manual_mapping", _no_ui)

    # Create a separate working copy under tmp_path for each case.
    case_tmp_dir = tmp_path / case_id.replace("/", "_")
    case_tmp_dir.mkdir(parents=True, exist_ok=True)

    tmp_input = case_tmp_dir / input_file.name
    tmp_meta = case_tmp_dir / meta_file.name
    shutil.copy2(input_file, tmp_input)
    shutil.copy2(meta_file, tmp_meta)

    # Call the CLI as the user would.
    argv = [
        "geo-mapper",
        "--data",
        str(tmp_input),
        "--yaml",
        str(tmp_meta),
        "--auto-export-source",
        "true",
    ]
    monkeypatch.setattr("sys.argv", argv)
    cli.main()

    # The CLI writes the results to <input_dir>/results_<stem>.
    generated_results_dir = case_tmp_dir / expected_results_dir.name
    assert generated_results_dir.is_dir(), (
        f"[{case_id}] Expected results directory {generated_results_dir} "
        f"to be created by CLI."
    )

    def _load_sorted_csv(path: Path) -> pd.DataFrame:
        df = pd.read_csv(path)
        if not df.columns.empty:
            df = df.sort_values(list(df.columns)).reset_index(drop=True)
        return df

    for filename in ("mapped_pairs.csv", "unmapped_geodata.csv", "unmapped_orginal.csv"):
        expected_path = expected_results_dir / filename
        generated_path = generated_results_dir / filename
        assert expected_path.is_file(), f"[{case_id}] Missing expected file: {expected_path}"
        assert generated_path.is_file(), f"[{case_id}] Missing generated file: {generated_path}"

        expected_df = _load_sorted_csv(expected_path)
        generated_df = _load_sorted_csv(generated_path)

        # Columns and values must match (data types may differ slightly).
        pd.testing.assert_frame_equal(
            expected_df,
            generated_df,
            check_dtype=False,
            obj=f"[{case_id}] {filename}",
        )
