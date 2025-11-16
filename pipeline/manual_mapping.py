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
)
from .constants import (
    PROMPT_MANUAL_SELECT_INPUT,
    PROMPT_MANUAL_SEARCH_GEODATA,
    PROMPT_MANUAL_SELECT_GEODATA,
    LABEL_MANUAL_UNDO_LAST,
    LABEL_MANUAL_DONE,
    LABEL_MANUAL_NO_MAPPING,
    CURSES_MANUAL_HELP,
)


logger = logging.getLogger(__name__)


# Sentinel values used in the manual mapping UI.
_DONE_SENTINEL = "__MANUAL_MAPPING_DONE__"
_UNDO_SENTINEL = "__MANUAL_MAPPING_UNDO__"


def _manual_mapping_curses_loop(
    stdscr,
    dataframe: pd.DataFrame,
    mapping_df: pd.DataFrame,
    geodata_frame: pd.DataFrame,
    source_path: str,
    source_col: str,
) -> pd.DataFrame:
    """Curses-based UI with two panes: input CSV on the left, geodata on the right.

    This view is used to manually connect remaining unmapped input rows to
    geodata rows. It keeps already-used geodata IDs unique within a CSV.
    """

    curses.curs_set(0)
    stdscr.nodelay(False)
    stdscr.keypad(True)

    # Initial lists and current filter terms for both panes
    input_search = ""
    geo_search = ""

    def _build_lists() -> Tuple[List[Tuple[object, str]], List[Tuple[str, str]]]:
        """Build filtered lists for CSV inputs and available geodata rows."""
        # Unmapped input rows
        unmapped_mask = mapping_df["mapped_value"].isna()
        input_items: List[Tuple[object, str]] = []
        for idx in mapping_df.index[unmapped_mask]:
            val = str(dataframe.loc[idx, source_col])
            if input_search and input_search.lower() not in val.lower():
                continue
            input_items.append((idx, val))

        # Geodata IDs that are still free (not used in this CSV)
        used_ids = set(str(v) for v in mapping_df["mapped_value"].dropna().astype(str))
        geo_items: List[Tuple[str, str]] = []
        if {"id", "name"}.issubset(geodata_frame.columns):
            for gid, name in zip(geodata_frame["id"], geodata_frame["name"], strict=False):
                if pd.isna(gid):
                    continue
                gid_str = str(gid)
                if gid_str in used_ids:
                    continue
                name_str = str(name)
                if geo_search and geo_search.lower() not in name_str.lower():
                    continue
                geo_items.append((gid_str, name_str))
        return input_items, geo_items

    input_items, geo_items = _build_lists()

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

        # Header line describing which geodata CSV we are mapping against
        header = f"Manual mapping for: {source_path}"
        stdscr.addnstr(0, 0, header, width - 1)
        stdscr.addnstr(1, 0, message, width - 1)

        # Panel titles (labels; focus is highlighted on individual rows)
        left_title = " CSV inputs (unmapped) "
        right_title = " Geodata (still unused) "
        stdscr.addnstr(3, 0, left_title, max(0, mid - 1))
        stdscr.addnstr(3, mid + 1, right_title, max(0, width - mid - 2))

        # Vertical separator line between the panes
        try:
            stdscr.vline(0, mid, curses.ACS_VLINE, height)
        except Exception:
            # Fallback if ACS_VLINE is not available
            for y in range(0, height):
                stdscr.addch(y, mid, "|")

        # Left pane: CSV inputs
        for row in range(max_rows):
            idx_in_list = left_offset + row
            if idx_in_list >= len(input_items):
                break
            idx, val = input_items[idx_in_list]
            is_cur = (idx_in_list == left_cursor)
            prefix = "> " if is_cur else "  "
            line = f"{prefix}[{idx}] {val}"
            attr = curses.A_REVERSE if (focus == "left" and is_cur) else 0
            stdscr.addnstr(4 + row, 0, line, max(0, mid - 1), attr)

        # Right pane: geodata candidates
        for row in range(max_rows):
            idx_in_list = right_offset + row
            if idx_in_list >= len(geo_items):
                break
            gid, name = geo_items[idx_in_list]
            is_cur = (idx_in_list == right_cursor)
            prefix = "> " if is_cur else "  "
            line = f"{prefix}{gid} – {name}"
            attr = curses.A_REVERSE if (focus == "right" and is_cur) else 0
            stdscr.addnstr(4 + row, mid + 1, line, max(0, width - mid - 2), attr)

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
            stdscr.addnstr(height - 1, 0, prompt, width - 1)
            curses.echo()
            try:
                query_bytes = stdscr.getstr(height - 1, len(prompt), max(1, width - len(prompt) - 1))
                query = query_bytes.decode("utf-8", errors="ignore")
            except Exception:
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
            gid, name = geo_items[right_cursor]

            # Write mapping into the mapping_df
            mapping_df.loc[row_idx, "mapped_by"] = "manual"
            mapping_df.loc[row_idx, "mapped_value"] = gid
            mapping_df.loc[row_idx, "mapped_source"] = source_path
            mapping_df.loc[row_idx, "mapped_label"] = name
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

        choices: List[Choice] = []
        if history:
            choices.append(Choice(LABEL_MANUAL_UNDO_LAST, value=_UNDO_SENTINEL))
        for idx in unmapped_indices[:30]:
            val = dataframe.loc[idx, source_col]
            choices.append(Choice(f"[{idx}] {val}", value=idx))
        choices.append(Choice(LABEL_MANUAL_DONE, value=_DONE_SENTINEL))

        selected = questionary.select(
            PROMPT_MANUAL_SELECT_INPUT,
            choices=choices,
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

        search = questionary.text(PROMPT_MANUAL_SEARCH_GEODATA).ask()

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
        for _, row in candidates.head(30).iterrows():
            gid = str(row["id"])
            name = str(row["name"])
            choices.append(Choice(f"{gid} – {name}", value=(gid, name)))
        choices.append(Choice(LABEL_MANUAL_NO_MAPPING, value=(None, None)))

        selected = questionary.select(
            PROMPT_MANUAL_SELECT_GEODATA, choices=choices
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


def _select_unmapped_row(mapping_df: pd.DataFrame, dataframe: pd.DataFrame, source_col: str) -> object | None:
    """Let the user pick one unmapped input row (by index) to map."""
    unmapped_mask = mapping_df["mapped_value"].isna()
    unmapped_indices = list(mapping_df.index[unmapped_mask])
    if not unmapped_indices:
        return None

    # Build a concise selection list (max 30 items)
    from questionary import Choice

    choices: List[Choice] = []
    for idx in unmapped_indices[:30]:
        val = dataframe.loc[idx, source_col]
        choices.append(Choice(f"[{idx}] {val}", value=idx))
    choices.append(Choice(LABEL_MANUAL_DONE, value=_DONE_SENTINEL))

    selected = questionary.select(
        PROMPT_MANUAL_SELECT_INPUT,
        choices=choices,
    ).ask()
    return selected


def _select_geodata_target(
    geodata_frame: pd.DataFrame, mapping_df: pd.DataFrame
) -> Tuple[str, str] | Tuple[None, None]:
    """Let the user select a geodata row (id, name) as mapping target.

    Only geodata entries whose ID has not yet been used in this CSV
    (neither automatically nor manually) are offered.
    """
    if geodata_frame.empty or not {"id", "name"}.issubset(geodata_frame.columns):
        return None, None

    # Filter out IDs that have already been mapped
    used_ids = set(str(v) for v in mapping_df["mapped_value"].dropna().astype(str))
    if used_ids:
        base_candidates = geodata_frame[~geodata_frame["id"].astype(str).isin(used_ids)]
    else:
        base_candidates = geodata_frame
    if base_candidates.empty:
        logger.info("All geodata entries in this CSV are already mapped.")
        return None, None

    # Optional search term to narrow down candidates
    search = questionary.text(PROMPT_MANUAL_SEARCH_GEODATA).ask()

    candidates = base_candidates
    if search:
        mask = base_candidates["name"].astype(str).str.contains(search, case=False, na=False)
        candidates = base_candidates[mask]
    if candidates.empty:
        logger.info("No geodata candidates found for the search term.")
        return None, None

    # Show at most 30 candidates in the menu
    from questionary import Choice

    choices: List[Choice] = []
    for _, row in candidates.head(30).iterrows():
        gid = str(row["id"])
        name = str(row["name"])
        choices.append(Choice(f"{gid} – {name}", value=(gid, name)))
    choices.append(Choice(LABEL_MANUAL_NO_MAPPING, value=(None, None)))

    selected = questionary.select(
        PROMPT_MANUAL_SELECT_GEODATA, choices=choices
    ).ask()

    if not selected:
        return None, None
    return selected


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
    except Exception as exc:
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
