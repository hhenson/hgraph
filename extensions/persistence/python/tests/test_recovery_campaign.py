"""A sample of the recovery campaign, on every test run.

The campaign proper is nightly (``.github/workflows/recovery-nightly.yml``; developer guide,
``testing.rst``, "Recovery campaign"). This keeps its machinery -- generation, child
interpreters, worker processes, the verdict -- from being first exercised at night, and
catches a recovery regression in the shapes a pull request is most likely to break.
"""

import json
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]

pytestmark = pytest.mark.skipif(
    not (ROOT / "tools" / "recovery" / "cli.py").is_file(),
    reason="the campaign lives in the repository's tools/, which an installed package does not carry")


def test_a_sample_of_the_pull_request_profile_recovers(tmp_path, monkeypatch):
    # Child interpreters and dmap_/spawn_ worker processes find ``tools.recovery`` by name.
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(filter(None, [str(ROOT), os.environ.get("PYTHONPATH")])))
    monkeypatch.syspath_prepend(str(ROOT))
    from tools.recovery import cli

    # Every fifth scenario of the profile: each leaf, layer, host and mode, in a few seconds.
    code = cli.main(["campaign", "--profile", "pr", "--seed", "20260919", "--shard-index", "0",
                     "--shard-count", "5", "--control-every", "2", "--output-dir", str(tmp_path)])
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["failures"] == [], [failure["detail"] for failure in report["failures"]]
    assert code == 0
    assert report["run"] >= 15
    # Snapshot AND recover ran, and in each some control differed: neither compared nothing.
    for mode in ("snapshot", "recover"):
        assert report["modes"][mode]["run"] > 0, report["modes"]
        assert report["modes"][mode]["sensitive"] > 0, report["modes"]
    assert {"map", "mesh", "dmapi"} <= set(report["coverage"]["layer"]), report["coverage"]
    assert {"graph", "spawn"} <= set(report["coverage"]["host"]), report["coverage"]
