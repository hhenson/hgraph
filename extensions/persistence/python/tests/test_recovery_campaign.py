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


def _campaign(monkeypatch):
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(filter(None, [str(ROOT), os.environ.get("PYTHONPATH")])))
    monkeypatch.syspath_prepend(str(ROOT))


def test_a_hung_scenario_is_cut_off_after_one_scenarios_allowance(tmp_path, monkeypatch):
    # Reports one result, then hangs. The supervisor has to keep the result and kill the
    # child after ONE scenario's allowance -- not after the whole batch's, which let a
    # single early hang burn most of a shard's job before anything was isolated.
    _campaign(monkeypatch)
    import sys
    import time

    from tools.recovery import cli

    output = tmp_path / "batch.json"
    child = ("import json, pathlib, sys, time; "
             "pathlib.Path(sys.argv[1]).write_text(json.dumps([{'status': 'pass'}])); time.sleep(600)")
    started = time.monotonic()
    done, failure = cli._supervise([sys.executable, "-c", child, str(output)], output, timeout=1.0, startup=5.0)
    assert done == [{"status": "pass"}]
    assert "hung" in failure
    assert time.monotonic() - started < 15


def test_a_mismatch_without_the_familys_signature_is_a_failure(tmp_path, monkeypatch):
    # Family membership is a relation over the RECIPE: "this scenario can reach the defect".
    # It says nothing about what went wrong, so a mismatch is filed as known only when it has
    # the defect's signature. Otherwise any regression that happened to sit in a member
    # scenario would leave the nightly green.
    _campaign(monkeypatch)
    from tools.recovery import run as runner
    from tools.recovery.model import MESH_EMPTY_INPUT, TSD_SLOT_ORDER, Scenario, known_defect

    # In the slot-order family (a removal on each side of the cut), and restarting invisibly.
    member = Scenario(chain="folded", placement="outer", host="graph", mode="snapshot",
                      events=({2: 2, 5: 1}, {2: "REMOVE"}, {1: 4}, {5: "REMOVE"}), cuts=(3,))
    assert known_defect(member) is TSD_SLOT_ORDER
    honest = runner.run(member)
    assert honest.status == "pass", honest.detail
    # The same scenario with a WRONG answer that key order cannot explain: the probe shows
    # the restored input iterating as the unbroken one does, so this is something else.
    judged = runner._classify(member, tmp_path, [1, 2, 3, 4], [1, 2, 3, 99], "", lambda: 0.0)
    assert judged.status == "fail"
    assert "does NOT have its signature" in judged.detail

    # The mesh_ family: only keys holding an EMPTY collection may be missing afterwards.
    mesh = Scenario(chain="map__mesh__doubled", placement="outer", host="graph", mode="recover",
                    events=({0: {1: 4}, 5: {1: 1}}, {0: {1: "REMOVE"}}, None, {5: {1: 3}}), cuts=(2,))
    assert known_defect(mesh) is MESH_EMPTY_INPUT
    unbroken = [((0, ()), (5, ((1, 2),)))]
    assert runner._classify(mesh, tmp_path, unbroken, [((5, ((1, 2),)),)], "", lambda: 0.0).status == "known"
    assert runner._classify(mesh, tmp_path, unbroken, [((5, ((1, 7),)),)], "", lambda: 0.0).status == "fail"
