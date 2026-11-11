#!/usr/bin/env python3
"""Run Gate 1 from a clean checkout without installing the package."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))

from dental_market.cli import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main(["m1-gate", *sys.argv[1:]]))
