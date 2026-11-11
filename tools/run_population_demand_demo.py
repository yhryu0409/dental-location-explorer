#!/usr/bin/env python3
"""Generate a self-contained demand report from the project's synthetic example."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dental_market.population_demand_mvp import compute, write_release


def load_input(relative_path: str) -> tuple[dict, dict]:
    raw = (ROOT / relative_path).read_bytes()
    provenance = {"path": relative_path, "sha256": hashlib.sha256(raw).hexdigest()}
    return json.loads(raw), provenance


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-root", type=Path, default=Path("var/population_demand_demo"),
        help="Directory for the content-addressed report (default: %(default)s)",
    )
    args = parser.parse_args()
    data, fixture_source = load_input("fixtures/population_demand_mvp_demo.json")
    config, config_source = load_input("metadata/population_demand_mvp_v1.json")
    if data.get("mode") != "synthetic_demo":
        parser.error("the example input must declare mode synthetic_demo")

    result = compute(data["markets"], data["populations"], data["residential_support"], config)
    result["inputs"] = {"mode": data["mode"], "custom": fixture_source, "config": config_source}
    release = write_release(
        result, args.output_root, title="인구·음식 집객·치과 수요 MVP — 가상 입력 예제"
    )
    summary = {
        "status": result["status"],
        "mode": data["mode"],
        "market_count": len(result["markets"]),
        "population_area_count": len(data["populations"]),
        "allocated_cell_count": len(result["population_allocation"]["cells"]),
        "release": str(release),
        "report": str(release / "report.html"),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
