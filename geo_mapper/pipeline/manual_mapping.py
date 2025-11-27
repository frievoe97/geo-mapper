"""Interactive manual mapping step for a single selected geodata CSV."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Tuple

import curses
import pandas as pd
import questionary

from .storage import (
    get_export_geodata_source,
    get_geodata_mappings,
    set_geodata_mappings,
    get_geodata_frames,
    get_selections,
    get_meta_config,
)
from .constants import (
    PROMPT_MANUAL_SELECT_INPUT,
    PROMPT_MANUAL_SEARCH_GEODATA,
    PROMPT_MANUAL_SELECT_GEODATA,
    LABEL_MANUAL_UNDO_LAST,
    LABEL_MANUAL_DONE,
    LABEL_MANUAL_NO_MAPPING,
    CURSES_MANUAL_HELP,
    GEODATA_ID_COLUMNS,
    infer_dataset_family,
)
from .utils.ui import DEFAULT_STYLE


logger = logging.getLogger(__name__)


# Sentinel values used in the manual mapping UI.
_DONE_SENTINEL = "__MANUAL_MAPPING_DONE__"
_UNDO_SENTINEL = "__MANUAL_MAPPING_UNDO__"


def _apply_meta_manual_mappings(
    dataframe: pd.DataFrame,
    mapping_df: pd.DataFrame,
    geodata_frame: pd.DataFrame,
    source_path: str,
) -> pd.DataFrame:
    """Apply manual mappings defined in the meta configuration, if present.

    This uses the same effective ID/name columns as the main pipeline and only
    fills rows that are still unmapped. Geodata IDs that are already taken
    are not assigned again.
    """

    meta = get_meta_config()
    if not isinstance(meta, dict):
        return mapping_df
    manual_entries = meta.get("manual_mappings")
    if not manual_entries:
        return mapping_df

    selections = get_selections()
    id_cols = getattr(selections, "id_columns", None) or (
        [selections.id_column] if selections.id_column else []
    )
    name_col = (
        selections.name_column
        or selections.column
        or (dataframe.columns[0] if len(dataframe.columns) else None)
    )
    if not id_cols and not name_col:
        return mapping_df

    if "mapped_value" not in mapping_df.columns:
        return mapping_df

    used_ids = set(str(v) for v in mapping_df["mapped_value"].dropna().astype(str))

    def _norm_val(val: object) -> object:
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except TypeError:
            pass
        return str(val)

    # Optional: ensure that the geodata ID exists
    available_ids = set()
    if "id" in geodata_frame.columns:
        available_ids = set(str(v) for v in geodata_frame["id"].dropna().astype(str))

    for entry in manual_entries:
        if not isinstance(entry, dict):
            continue
        input_ids_spec = entry.get("input_ids") or {}
        geodata_id = entry.get("geodata_id")
        geodata_name = entry.get("geodata_name")
        input_name_expected = _norm_val(entry.get("input_name"))

        if geodata_id is None:
            continue
        gid_str = str(geodata_id)
        if available_ids and gid_str not in available_ids:
            continue
        if gid_str in used_ids:
            continue

        # Target ID values for each ID column (in the current order of id_cols)
        expected_ids = []
        for i, _col in enumerate(id_cols):
            val = input_ids_spec.get(str(i))
            expected_ids.append(_norm_val(val))

        unmapped_mask = mapping_df["mapped_value"].isna()
        candidate_indices = list(mapping_df.index[unmapped_mask])
        for idx in candidate_indices:
            # Compare IDs
            id_match = True
            for i, col in enumerate(id_cols):
                row_val = None
                if col in dataframe.columns:
                    row_val = dataframe.loc[idx, col]
                row_val = _norm_val(row_val)
                expected = expected_ids[i]
                if expected is not None and row_val != expected:
                    id_match = False
                    break
            if not id_match:
                continue

            # Compare names (if present both in the entry and in the DataFrame)
            if name_col and name_col in dataframe.columns and input_name_expected is not None:
                row_name = _norm_val(dataframe.loc[idx, name_col])
                if row_name != input_name_expected:
                    continue

            # Apply mapping
            mapping_df.loc[idx, "mapped_by"] = "manual"
            mapping_df.loc[idx, "mapped_value"] = gid_str
            mapping_df.loc[idx, "mapped_source"] = source_path
            mapping_df.loc[idx, "mapped_label"] = geodata_name
            if "mapped_param" in mapping_df.columns:
                mapping_df.loc[idx, "mapped_param"] = pd.NA
            used_ids.add(gid_str)
            # Map at most one row per manual entry
            break

    return mapping_df


def _manual_mapping_curses_loop(
    stdscr,
    dataframe: pd.DataFrame,
    mapping_df: pd.DataFrame,
    geodata_frame: pd.DataFrame,
    source_path: str,
    source_col: str,
) -> pd.DataFrame:
    """Curses-based UI with two panes: input data on the left, geodata on the right.

    This view is used to manually connect remaining unmapped input rows to
    geodata rows. It keeps already-used geodata IDs unique within a CSV.
    """

    curses.curs_set(0)
    stdscr.nodelay(False)
    stdscr.keypad(True)

    selections = get_selections()
    # Input ID/name columns
    id_cols = getattr(selections, "id_columns", None) or (
        [selections.id_column] if selections.id_column else []
    )
    name_col = (
        selections.name_column
        or selections.column
        or (dataframe.columns[0] if len(dataframe.columns) else None)
    )

    # Determine geodata ID columns based on dataset family
    dataset_family = infer_dataset_family(source_path) or ""
    geodata_id_cols = [src for src, _export in GEODATA_ID_COLUMNS.get(dataset_family, [])]
    if not geodata_id_cols:
        # Fallback: use the canonical 'id' column for display and matching
        geodata_id_cols = ["id"]

    # Initial lists and current filter terms for both panes
    input_search = ""
    geo_search = ""

    def _build_lists() -> Tuple[List[Tuple[object, str]], List[Tuple[str, str, str]]]:
        """Build filtered and column-aligned lists for input rows and available geodata rows.

        Returns:
        - input_rows:  [(row_index, display_string), ...]
        - geo_rows:    [(canonical_geodata_id, geodata_name, display_string), ...]
        """
        # Unmapped input rows
        unmapped_mask = mapping_df["mapped_value"].isna()
        input_rows: List[Tuple[object, str]] = []
        for row_index in mapping_df.index[unmapped_mask]:
            parts: list[str] = []
            # IDs of the input data (if present)
            for col in id_cols:
                if col in dataframe.columns:
                    val = dataframe.loc[row_index, col]
                    if not (isinstance(val, float) and pd.isna(val)):
                        parts.append(str(val))
            # Name of the input data (if present)
            name_val = None
            if name_col and name_col in dataframe.columns:
                name_val = dataframe.loc[row_index, name_col]
            elif source_col in dataframe.columns:
                name_val = dataframe.loc[row_index, source_col]
            if name_val is not None and not (isinstance(name_val, float) and pd.isna(name_val)):
                parts.append(str(name_val))
            display = " | ".join(parts) if parts else ""
            sort_text = "" if name_val is None or (isinstance(name_val, float) and pd.isna(name_val)) else str(name_val)
            text_for_search = sort_text or display
            if input_search and input_search.lower() not in text_for_search.lower():
                continue
            input_rows.append((row_index, display))

        # Sort alphabetically by name (if present), otherwise by display text
        def _input_sort_key(item: Tuple[object, str]) -> str:
            idx, display = item
            if name_col and name_col in dataframe.columns:
                val = dataframe.loc[idx, name_col]
                return ("" if (isinstance(val, float) and pd.isna(val)) else str(val)).casefold()
            return display.casefold()

        input_rows.sort(key=_input_sort_key)

        # Geodata IDs that are still free (not used in this CSV)
        used_ids = set(str(v) for v in mapping_df["mapped_value"].dropna().astype(str))
        raw_geo_rows: List[Tuple[str, List[str]]] = []
        if "name" in geodata_frame.columns and "id" in geodata_frame.columns:
            for idx, canonical_id in geodata_frame["id"].items():
                if pd.isna(canonical_id):
                    continue
                geo_id_str = str(canonical_id)
                if geo_id_str in used_ids:
                    continue
                cols: list[str] = []
                for src_col in geodata_id_cols:
                    val = geodata_frame.at[idx, src_col] if src_col in geodata_frame.columns else None
                    cols.append("" if pd.isna(val) else str(val))
                geo_name_str = str(geodata_frame.at[idx, "name"])
                cols.append(geo_name_str)
                text_for_search = " ".join(cols)
                if geo_search and geo_search.lower() not in text_for_search.lower():
                    continue
                raw_geo_rows.append((geo_id_str, cols))

        geo_rows: List[Tuple[str, str, str]] = []
        if raw_geo_rows:
            # Sort by name (last column)
            raw_geo_rows.sort(key=lambda pair: pair[1][-1].casefold())
            for geo_id_str, cols in raw_geo_rows:
                display = " | ".join(cols)
                geodata_name = cols[-1] if cols else ""
                geo_rows.append((geo_id_str, geodata_name, display))

        return input_rows, geo_rows

    input_items, geo_items = _build_lists()

    def _safe_addnstr(y: int, x: int, text: str, max_len: int, attr: int = 0) -> None:
        """Safely write text into the curses window without raising errors.

        This guards against very small terminals or edge-case coordinates so that
        the curses UI does not fall back to the dialog-based variant unless the
        environment truly does not support curses.
        """
        if max_len <= 0:
            return
        height, width = stdscr.getmaxyx()
        if y < 0 or y >= height or x < 0 or x >= width:
            return
        try:
            stdscr.addnstr(y, x, text, max_len, attr)
        except curses.error:
            # Ignore drawing errors; they only affect presentation, not logic.
            pass

    # Configure color attributes so the manual mapping UI is as readable
    # as the questionary-based prompts (stronger contrast for headers and
    # clearly highlighted selections).
    header_attr = curses.A_BOLD
    title_attr = curses.A_BOLD
    normal_attr = 0
    selected_attr = curses.A_REVERSE | curses.A_BOLD
    try:
        if curses.has_colors():
            curses.start_color()
            default_bg = -1
            try:
                curses.use_default_colors()
            except curses.error:
                default_bg = curses.COLOR_BLACK
            curses.init_pair(1, curses.COLOR_CYAN, default_bg)
            curses.init_pair(2, curses.COLOR_WHITE, default_bg)
            curses.init_pair(3, curses.COLOR_YELLOW, default_bg)
            header_attr = curses.color_pair(1) | curses.A_BOLD
            title_attr = curses.color_pair(1) | curses.A_BOLD
            normal_attr = curses.color_pair(2)
            selected_attr = curses.color_pair(3) | curses.A_BOLD
    except curses.error:
        # Fall back to attribute-only styling if colors are not supported.
        header_attr = curses.A_BOLD
        title_attr = curses.A_BOLD
        normal_attr = 0
        selected_attr = curses.A_REVERSE | curses.A_BOLD

    left_cursor = 0
    right_cursor = 0
    left_offset = 0
    right_offset = 0
    focus = "left"  # or "right"

    # Stack for undo: stores indices of the most recently mapped input rows
    history: List[object] = []

    message = CURSES_MANUAL_HELP

    while True:
        height, width = stdscr.getmaxyx()
        stdscr.erase()

        mid = width // 2
        max_rows = max(1, height - 4)

        # Header line describing which geodata source we are mapping against
        header = f"Manual mapping:"
        _safe_addnstr(0, 0, header, width - 1, header_attr)
        _safe_addnstr(1, 0, message, width - 1, normal_attr)

        # Panel titles (labels; focus is highlighted on individual rows)
        left_title = " Input data (unmapped) "
        right_title = " Geodata (still unused) "
        _safe_addnstr(3, 0, left_title, max(0, mid - 1), title_attr)
        _safe_addnstr(3, mid + 1, right_title, max(0, width - mid - 2), title_attr)

        # Vertical separator line between the panes
        try:
            stdscr.vline(0, mid, curses.ACS_VLINE, height)
        except curses.error:
            # Fallback if ACS_VLINE is not available
            for y in range(0, height):
                try:
                    stdscr.addch(y, mid, "|")
                except curses.error:
                    break

        # Left pane: input values
        # Header with ID and name columns (if available)
        input_header_parts: list[str] = []
        for col in id_cols:
            input_header_parts.append(str(col))
        if name_col:
            input_header_parts.append(str(name_col))
        input_header = " | ".join(input_header_parts) if input_header_parts else "value"
        _safe_addnstr(4, 0, input_header, max(0, mid - 1), title_attr)

        data_start_row = 5
        for row in range(max_rows):
            idx_in_list = left_offset + row
            if idx_in_list >= len(input_items):
                break
            idx, val = input_items[idx_in_list]
            is_cur = (idx_in_list == left_cursor)
            prefix = "> " if is_cur else "  "
            line = f"{prefix}{val}"
            attr = selected_attr if (focus == "left" and is_cur) else normal_attr
            _safe_addnstr(data_start_row + row, 0, line, max(0, mid - 1), attr)

        # Right pane: geodata candidates
        geo_header_parts = [col for col in geodata_id_cols]
        geo_header_parts.append("name")
        geo_header = " | ".join(geo_header_parts)
        _safe_addnstr(4, mid + 1, geo_header, max(0, width - mid - 2), title_attr)
        for row in range(max_rows):
            idx_in_list = right_offset + row
            if idx_in_list >= len(geo_items):
                break
            _gid, _geo_name, display = geo_items[idx_in_list]
            is_cur = (idx_in_list == right_cursor)
            prefix = "> " if is_cur else "  "
            line = f"{prefix}{display}"
            attr = selected_attr if (focus == "right" and is_cur) else normal_attr
            _safe_addnstr(data_start_row + row, mid + 1, line, max(0, width - mid - 2), attr)

        stdscr.refresh()

        ch = stdscr.getch()

        # Exit
        if ch in (ord("q"), 27):  # q or ESC
            break

        # Switch focus between left and right pane
        if ch in (curses.KEY_LEFT, curses.KEY_RIGHT, 9):  # 9 = TAB
            focus = "right" if focus == "left" else "left"
            continue

        # Navigation within the focused pane
        if ch in (curses.KEY_UP, ord("k")):
            if focus == "left" and input_items:
                if left_cursor > 0:
                    left_cursor -= 1
                if left_cursor < left_offset:
                    left_offset = left_cursor
            elif focus == "right" and geo_items:
                if right_cursor > 0:
                    right_cursor -= 1
                if right_cursor < right_offset:
                    right_offset = right_cursor
            continue

        if ch in (curses.KEY_DOWN, ord("j")):
            if focus == "left" and input_items:
                if left_cursor < len(input_items) - 1:
                    left_cursor += 1
                if left_cursor >= left_offset + max_rows:
                    left_offset = left_cursor - max_rows + 1
            elif focus == "right" and geo_items:
                if right_cursor < len(geo_items) - 1:
                    right_cursor += 1
                if right_cursor >= right_offset + max_rows:
                    right_offset = right_cursor - max_rows + 1
            continue

        # Search within the currently focused pane
        if ch == ord("/"):
            # Input prompt at the bottom of the screen
            prompt = "Search CSV: " if focus == "left" else "Search geodata: "
            stdscr.move(height - 1, 0)
            stdscr.clrtoeol()
            _safe_addnstr(height - 1, 0, prompt, width - 1)
            curses.echo()
            try:
                query_bytes = stdscr.getstr(
                    height - 1, len(prompt), max(1, width - len(prompt) - 1)
                )
                query = query_bytes.decode("utf-8", errors="ignore")
            except (curses.error, ValueError):
                query = ""
            finally:
                curses.noecho()

            if focus == "left":
                input_search = query.strip()
                left_cursor = 0
                left_offset = 0
            else:
                geo_search = query.strip()
                right_cursor = 0
                right_offset = 0

            input_items, geo_items = _build_lists()
            continue

        # Perform a mapping between the currently selected input row and geodata row
        if ch in (curses.KEY_ENTER, 10, 13):
            if not input_items or not geo_items:
                continue
            if left_cursor >= len(input_items) or right_cursor >= len(geo_items):
                continue

            row_idx, _val = input_items[left_cursor]
            gid, geo_name, _display = geo_items[right_cursor]

            # Write mapping into the mapping_df
            mapping_df.loc[row_idx, "mapped_by"] = "manual"
            mapping_df.loc[row_idx, "mapped_value"] = gid
            mapping_df.loc[row_idx, "mapped_source"] = source_path
            mapping_df.loc[row_idx, "mapped_label"] = geo_name
            if "mapped_param" in mapping_df.columns:
                mapping_df.loc[row_idx, "mapped_param"] = pd.NA

            # Record this mapping for undo
            history.append(row_idx)

            # After a mapping, clear filters so all remaining entries are visible again
            input_search = ""
            geo_search = ""
            # Rebuild lists so that newly used IDs are no longer offered
            input_items, geo_items = _build_lists()

            # Adjust cursors to stay within bounds if list sizes shrank
            if left_cursor >= len(input_items):
                left_cursor = max(0, len(input_items) - 1)
                left_offset = max(0, min(left_offset, left_cursor))
            if right_cursor >= len(geo_items):
                right_cursor = max(0, len(geo_items) - 1)
                right_offset = max(0, min(right_offset, right_cursor))

            # Stop once there is nothing left to map
            if not input_items or not geo_items:
                break

            continue

        # Undo the last mapping
        if ch in (ord("u"),):
            if not history:
                continue
            row_idx = history.pop()
            # Reset mapping for the last mapped index
            mapping_df.loc[row_idx, "mapped_by"] = pd.NA
            mapping_df.loc[row_idx, "mapped_value"] = pd.NA
            mapping_df.loc[row_idx, "mapped_source"] = pd.NA
            mapping_df.loc[row_idx, "mapped_label"] = pd.NA
            if "mapped_param" in mapping_df.columns:
                mapping_df.loc[row_idx, "mapped_param"] = pd.NA

            # Clear search filters and rebuild lists
            input_search = ""
            geo_search = ""
            input_items, geo_items = _build_lists()

            # Adjust cursor to point to an existing entry if needed
            if left_cursor >= len(input_items):
                left_cursor = max(0, len(input_items) - 1)
                left_offset = max(0, min(left_offset, left_cursor))
            if right_cursor >= len(geo_items):
                right_cursor = max(0, len(geo_items) - 1)
                right_offset = max(0, min(right_offset, right_cursor))

            continue

    return mapping_df


def _run_curses_manual_mapping(
    dataframe: pd.DataFrame,
    mapping_df: pd.DataFrame,
    geodata_frame: pd.DataFrame,
    source_path: str,
    source_col: str,
) -> pd.DataFrame:
    """Wrapper that starts the curses UI and returns the updated mapping DataFrame."""

    updated = {"mapping_df": mapping_df}

    def _runner(stdscr) -> None:
        updated["mapping_df"] = _manual_mapping_curses_loop(
            stdscr, dataframe, mapping_df, geodata_frame, source_path, source_col
        )

    curses.wrapper(_runner)
    return updated["mapping_df"]


def _run_questionary_manual_mapping(
    dataframe: pd.DataFrame,
    mapping_df: pd.DataFrame,
    geodata_frame: pd.DataFrame,
    source_path: str,
    source_col: str,
) -> pd.DataFrame:
    """Fallback: simple dialog-based manual mapping using questionary."""

    from questionary import Choice

    history: List[object] = []

    def _select_unmapped_row_local() -> object | None:
        unmapped_mask = mapping_df["mapped_value"].isna()
        unmapped_indices = list(mapping_df.index[unmapped_mask])
        if not unmapped_indices:
            return None

        def _is_empty_value(val: object) -> bool:
            if pd.isna(val):
                return True
            if isinstance(val, str) and not val.strip():
                return True
            text = str(val).strip().lower()
            return text in {"nan", "null", "none"}

        filtered: List[Tuple[object, str]] = []
        for idx in unmapped_indices:
            raw = dataframe.loc[idx, source_col] if source_col in dataframe.columns else None
            if _is_empty_value(raw):
                continue
            filtered.append((idx, str(raw)))
        if not filtered:
            return None

        sorted_unmapped = sorted(filtered, key=lambda item: item[1].casefold())

        choices: List[Choice] = []
        if history:
            choices.append(Choice(LABEL_MANUAL_UNDO_LAST, value=_UNDO_SENTINEL))
        for row_index, row_value in sorted_unmapped[:30]:
            choices.append(Choice(str(row_value), value=row_index))
        choices.append(Choice(LABEL_MANUAL_DONE, value=_DONE_SENTINEL))

        selected = questionary.select(
            PROMPT_MANUAL_SELECT_INPUT,
            choices=choices,
            style=DEFAULT_STYLE,
        ).ask()
        return selected

    def _select_geodata_target_local() -> Tuple[str, str] | Tuple[None, None]:
        if geodata_frame.empty or not {"id", "name"}.issubset(geodata_frame.columns):
            return None, None

        used_ids = set(str(v) for v in mapping_df["mapped_value"].dropna().astype(str))
        if used_ids:
            base_candidates = geodata_frame[
                ~geodata_frame["id"].astype(str).isin(used_ids)
            ]
        else:
            base_candidates = geodata_frame
        if base_candidates.empty:
            logger.info("All geodata entries in this CSV are already mapped.")
            return None, None

        search = questionary.text(
            PROMPT_MANUAL_SEARCH_GEODATA,
            style=DEFAULT_STYLE,
        ).ask()

        candidates = base_candidates
        if search:
            mask = base_candidates["name"].astype(str).str.contains(
                search, case=False, na=False
            )
            candidates = base_candidates[mask]
        if candidates.empty:
            logger.info("No geodata candidates found for the search term.")
            return None, None

        choices: List[Choice] = []
        geodata_options: List[Tuple[str, str, str]] = []
        # Build display with all relevant ID columns plus name
        dataset_family = infer_dataset_family(source_path) or ""
        geodata_id_cols = [src for src, _export in GEODATA_ID_COLUMNS.get(dataset_family, [])]
        if not geodata_id_cols:
            geodata_id_cols = ["id"]

        for _, row in candidates.iterrows():
            canonical_id = row.get("id")
            if pd.isna(canonical_id):
                continue
            geo_id = str(canonical_id)
            name_val = row.get("name")
            geo_name = "" if pd.isna(name_val) else str(name_val)
            id_parts: list[str] = []
            for src_col in geodata_id_cols:
                val = row.get(src_col)
                id_parts.append("" if pd.isna(val) else str(val))
            display_ids = " | ".join(id_parts)
            label = f"{display_ids} – {geo_name}" if display_ids else f"{geo_id} – {geo_name}"
            geodata_options.append((geo_id, geo_name, label))

        # Sort alphabetically by geodata name
        geodata_options.sort(key=lambda item: item[1].casefold())
        for geo_id, geo_name, label in geodata_options[:30]:
            choices.append(Choice(label, value=(geo_id, geo_name)))
        choices.append(Choice(LABEL_MANUAL_NO_MAPPING, value=(None, None)))

        selected = questionary.select(
            PROMPT_MANUAL_SELECT_GEODATA,
            choices=choices,
            style=DEFAULT_STYLE,
        ).ask()
        if not selected:
            return None, None
        return selected

    while True:
        idx = _select_unmapped_row_local()
        if idx is None or idx == _DONE_SENTINEL:
            break

        if idx == _UNDO_SENTINEL:
            if not history:
                continue
            row_idx = history.pop()
            mapping_df.loc[row_idx, "mapped_by"] = pd.NA
            mapping_df.loc[row_idx, "mapped_value"] = pd.NA
            mapping_df.loc[row_idx, "mapped_source"] = pd.NA
            mapping_df.loc[row_idx, "mapped_label"] = pd.NA
            if "mapped_param" in mapping_df.columns:
                mapping_df.loc[row_idx, "mapped_param"] = pd.NA
            continue

        gid, name = _select_geodata_target_local()
        if gid is None:
            continue

        mapping_df.loc[idx, "mapped_by"] = "manual"
        mapping_df.loc[idx, "mapped_value"] = gid
        mapping_df.loc[idx, "mapped_source"] = source_path
        mapping_df.loc[idx, "mapped_label"] = name
        if "mapped_param" in mapping_df.columns:
            mapping_df.loc[idx, "mapped_param"] = pd.NA

        history.append(idx)

        logger.info(
            "Manually mapped: [%s] %r -> %s (%s)",
            idx,
            dataframe.loc[idx, source_col],
            gid,
            name,
        )

    return mapping_df


def _find_geodata_frame(source_path: str) -> Tuple[Path, pd.DataFrame] | None:
    """Find the geodata frame corresponding to the given source path string."""
    for path, frame in get_geodata_frames():
        if str(path) == source_path:
            return path, frame
    return None


def manual_mapping_step(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Pipeline step: allow manual mappings for the selected geodata CSV."""
    source_path = get_export_geodata_source()
    if not source_path:
        # No export target selected -> manual mapping does not make sense
        logger.info("No geodata source selected for manual mapping – skipping this step.")
        return dataframe

    geodata_info = _find_geodata_frame(source_path)
    if geodata_info is None:
        logger.info("The selected geodata source could not be found in the loaded frames.")
        return dataframe
    _path, geodata_frame = geodata_info

    mappings_by_source = get_geodata_mappings()
    mapping_df = mappings_by_source.get(source_path)
    if mapping_df is None:
        logger.info("No mapping results available for %s.", source_path)
        return dataframe

    # Apply manual mappings from the meta configuration (if any)
    # before starting the interactive UI.
    mapping_df = _apply_meta_manual_mappings(
        dataframe,
        mapping_df,
        geodata_frame,
        source_path,
    )

    # If there are no unmapped input values left, there is nothing left
    # to do in the manual mapping step, so we can skip it.
    if "mapped_value" not in mapping_df.columns:
        logger.info(
            "Mapping results for %s do not contain a 'mapped_value' column – skipping manual mapping.",
            source_path,
        )
        return dataframe
    unmapped_mask = mapping_df["mapped_value"].isna()
    if not unmapped_mask.any():
        logger.info(
            "No unmapped input values for %s – skipping manual mapping.",
            source_path,
        )
        return dataframe

    if not {"id", "name"}.issubset(geodata_frame.columns):
        logger.info(
            "Geodata for %s lacks required 'id'/'name' columns – skipping manual mapping.",
            source_path,
        )
        return dataframe

    used_ids = set(str(v) for v in mapping_df["mapped_value"].dropna().astype(str))
    available_geodata = geodata_frame[geodata_frame["id"].notna()]
    if used_ids:
        available_geodata = available_geodata[
            ~available_geodata["id"].astype(str).isin(used_ids)
        ]
    if available_geodata.empty:
        logger.debug(
            "No unused geodata entries left for %s – skipping manual mapping.",
            source_path,
        )
        return dataframe

    selections = get_selections()
    source_col = (
        selections.name_column
        or selections.column
        or dataframe.columns[0]
    )

    try:
        # Preferred: curses-based two-pane UI for efficient mapping
        mapping_df = _run_curses_manual_mapping(
            dataframe, mapping_df, geodata_frame, source_path, source_col
        )
    except curses.error as exc:
        logger.info(
            "Curses UI for manual mapping is not available (%s) – falling back to a dialog-based variant.",
            exc,
        )
        mapping_df = _run_questionary_manual_mapping(
            dataframe, mapping_df, geodata_frame, source_path, source_col
        )

    # Write updated mappings back into central storage for later export
    mappings_by_source[source_path] = mapping_df
    set_geodata_mappings(mappings_by_source)

    return dataframe
