"""Compatibility launcher that delegates to the packaged CLI."""

from __future__ import annotations

from geo_mapper.cli import main


if __name__ == "__main__":
    main()
