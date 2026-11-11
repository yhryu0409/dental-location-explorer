"""Resolve research snapshots independently of the source checkout."""
import os
from pathlib import Path

DEFAULT_ROOT = Path(__file__).resolve().parents[2]
ROOT = Path(os.environ.get("DENTAL_EXPLORER_DATA_ROOT", DEFAULT_ROOT)).expanduser().resolve()
