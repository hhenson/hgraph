"""Contracts for the generated Python API inventory and typing surface."""

import ast
from pathlib import Path
import subprocess
import sys

import hgraph

from tools.api_inventory import (
    DEFAULT_RST,
    DEFAULT_STUB,
    collect_inventory,
)


def test_generated_api_inventory_is_current():
    # Other compatibility tests intentionally register Python overloads in the
    # process-global native registry. Check generated built-ins in a clean
    # interpreter so test ordering cannot change their common call shapes.
    result = subprocess.run(
        [sys.executable, str(DEFAULT_RST.parents[3] / "tools/api_inventory.py"), "--check"],
        cwd=DEFAULT_RST.parents[3],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    ast.parse(DEFAULT_STUB.read_text(encoding="utf-8"), filename=str(DEFAULT_STUB))


def test_root_exports_cover_supported_authoring_types_and_helpers():
    expected = {
        "Array",
        "CompoundScalar",
        "DebugContext",
        "LOGGER",
        "REF",
        "RecordReplayContext",
        "TSW",
        "collect",
        "combine",
        "compute_set_delta",
        "convert",
        "emit",
        "set_delta",
    }

    assert expected <= set(hgraph.__all__)


def test_lazy_operators_are_typed_without_expanding_wildcard_imports():
    inventory = collect_inventory()
    operator_names = {entry["name"] for entry in inventory["operators"]}

    assert "add_" in operator_names
    assert "add_" not in hgraph.__all__
    assert callable(hgraph.add_)
    assert "add_:" in DEFAULT_STUB.read_text(encoding="utf-8")


def test_typed_package_marker_is_present():
    marker = Path(hgraph.__file__).with_name("py.typed")
    assert marker.is_file()
