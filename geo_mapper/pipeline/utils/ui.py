"""Central UI helpers (e.g. neutral questionary styles)."""

from __future__ import annotations

from questionary import Style

# Neutral style that follows the terminal colors as closely as possible:
# - no explicit background colors
# - keep text colors at "default" and use only minimal highlighting
DEFAULT_STYLE = Style(
    [
        ("qmark", "bold"),
        ("question", "bold"),
        ("answer", "bold"),
        ("pointer", "bold"),
        ("highlighted", "bold"),
        ("selected", "reverse"),
        ("separator", ""),
        ("instruction", ""),
        ("text", ""),
        ("disabled", "fg:#888888"),
    ]
)
