"""Contracts for the generated Python API inventory and typing surface."""

import ast
from pathlib import Path
import re
import subprocess
import sys

import _hgraph
import hgraph

from tools.api_inventory import (
    DEFAULT_AUTHORING_API,
    DEFAULT_OPERATOR_CATALOGUE,
    DEFAULT_RST,
    DEFAULT_STUB,
    collect_authoring_api,
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


def test_operator_inventory_preserves_complete_native_overloads():
    inventory = collect_inventory()
    assert all(operator["documentation"] for operator in inventory["operators"])
    add = next(entry for entry in inventory["operators"] if entry["name"] == "add_")

    assert len(add["overloads"]) > 1
    assert any(
        overload["parameters"] == (
            {"name": "lhs", "kind": "time-series", "type_pattern": "TS[int]",
             "has_default": False},
            {"name": "rhs", "kind": "time-series", "type_pattern": "TS[int]",
             "has_default": False},
        )
        and overload["has_output"]
        and overload["output_pattern"] == "TS[int]"
        for overload in add["overloads"]
    )

    to_window = next(
        entry for entry in inventory["operators"] if entry["name"] == "to_window"
    )
    assert any(
        overload["parameters"][-1]["name"] == "reset"
        and overload["parameters"][-2]["has_default"]
        for overload in to_window["overloads"]
    )


def test_operator_catalogue_exposes_every_operator_signature_and_documentation():
    inventory = collect_inventory()
    source = DEFAULT_OPERATOR_CATALOGUE.read_text(encoding="utf-8")

    assert source.count(".. _python-operator-") == len(inventory["operators"])
    assert "add_(lhs: TS[int], rhs: TS[int]) -> TS[int]" in source
    assert "abs_(ts: TSL[S, SIZE]) -> OUT" in source
    assert "abs_(ts: S) -> OUT" in source
    assert ".. _python-operator-to_window:" in source
    assert "Accepted native overloads" in source
    assert not re.search(r"(?m)^   .*~[A-Za-z_]", source)
    assert ", 0]" not in source
    assert "__out__" not in source
    for operator in inventory["operators"]:
        assert operator["documentation"] in source


def test_authoring_reference_has_exact_signatures_and_parameter_docs():
    groups = collect_authoring_api()
    source = DEFAULT_AUTHORING_API.read_text(encoding="utf-8")
    entries = [entry for group in groups for entry in group["callables"]]

    assert ".. py:function:: hgraph.graph(fn=None, overloads=None" in source
    assert ".. py:class:: hgraph.GraphConfiguration(" in source
    assert ".. py:function:: hgraph.test.eval_node(" in source
    for entry in entries:
        assert entry["documentation"]
        assert all(
            f":param {parameter}:" in entry["documentation"]
            for parameter in entry["parameters"]
        )
        assert f"{entry['qualified_name']}{entry['signature']}" in source


def test_operator_stub_exposes_overloads_docs_and_every_public_operator():
    source = DEFAULT_STUB.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(DEFAULT_STUB))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }

    # hgraph.__init__ imports these declarations under TYPE_CHECKING. Every
    # annotation dependency must therefore be private or it becomes a false
    # top-level API (for example ``from hgraph import date`` in mypy).
    for statement in tree.body:
        if not isinstance(statement, ast.ImportFrom) or statement.module == "__future__":
            continue
        assert all(
            alias.asname is not None and alias.asname.startswith("_")
            for alias in statement.names
        )

    add = classes["_add__Operator"]
    add_calls = [
        node for node in add.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "__call__"
    ]
    assert len(add_calls) > 1
    assert any(
        [argument.arg for argument in call.args.args] == ["self", "lhs", "rhs"]
        and not call.args.defaults
        and call.args.vararg is None
        and call.args.kwarg is None
        for call in add_calls
    )
    assert "add_(lhs: TS[int], rhs: TS[int]) -> TS[int]" in ast.get_docstring(add)

    abs_operator = classes["_abs__Operator"]
    abs_documentation = ast.get_docstring(abs_operator)
    assert "abs_(ts: TSL[S, SIZE]) -> OUT" in abs_documentation
    assert "abs_(ts: S) -> OUT" in abs_documentation
    assert not re.search(r"~[A-Za-z_]", abs_documentation)
    assert ", 0]" not in abs_documentation
    assert "__out__" not in abs_documentation

    to_window = classes["_to_window_Operator"]
    assert any(
        [argument.arg for argument in call.args.kwonlyargs] == [
            "min_window_period", "reset"
        ]
        for call in to_window.body
        if isinstance(call, ast.FunctionDef) and call.name == "__call__"
    )

    typing_all = next(
        node for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "__all__"
                for target in node.targets)
    )
    typed_lazy_names = {element.value for element in typing_all.value.elts}
    public_registry_names = {
        name for name in _hgraph.operator_names() if not name.startswith("__")
    }
    explicitly_typed_names = public_registry_names & set(hgraph.__all__)
    assert typed_lazy_names | explicitly_typed_names == public_registry_names
    assert typed_lazy_names.isdisjoint(explicitly_typed_names)


def test_typed_package_marker_is_present():
    marker = Path(hgraph.__file__).with_name("py.typed")
    assert marker.is_file()
