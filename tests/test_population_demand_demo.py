"""Check the public CLI and its self-contained report using only the standard library."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest


ROOT = Path(__file__).resolve().parents[1]


class SyntheticReportTest(unittest.TestCase):
    def test_report_contains_the_verified_fixture_and_bundle(self):
        with TemporaryDirectory() as directory:
            stdout = subprocess.check_output(
                [sys.executable, str(ROOT / "tools/run_population_demand_demo.py")],
                cwd=directory, text=True,
            )
            summary = json.loads(stdout)
            self.assertEqual(summary["status"], "computed")
            self.assertEqual(summary["mode"], "synthetic_demo")
            self.assertEqual(summary["market_count"], 3)
            self.assertEqual(summary["population_area_count"], 2)
            self.assertEqual(summary["allocated_cell_count"], 6)

            release = Path(directory) / summary["release"]
            bundle_bytes = (release / "bundle.json").read_bytes()
            self.assertEqual(release.name, "pdm-" + hashlib.sha256(bundle_bytes).hexdigest()[:24])
            bundle = json.loads(bundle_bytes)
            report = (Path(directory) / summary["report"]).read_text(encoding="utf-8")
            embedded_json = report.split('<script type="application/json" id="data">', 1)[1]
            self.assertEqual(bundle, json.loads(embedded_json.split("</script>", 1)[0]))
            for key, path in (
                ("custom", "fixtures/population_demand_mvp_demo.json"),
                ("config", "metadata/population_demand_mvp_v1.json"),
            ):
                checksum = hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
                self.assertEqual(bundle["inputs"][key], {"path": path, "sha256": checksum})
            self.assertNotIn(str(ROOT), report)


if __name__ == "__main__":
    unittest.main()
