"""Contracts for the generated Python API inventory and typing surface."""

import ast
import os
from pathlib import Path
import pickle
import re
import subprocess
import sys

import _hgraph
import hgraph
from hgraph._operator_groups import OPERATOR_OVERRIDE_GROUPS, OPERATOR_OVERRIDE_NAMES
from hgraph._operator_signature import PublicTypePatternFormatter

from tools.api_inventory import (
    DEFAULT_AUTHORING_API,
    DEFAULT_OPERATOR_CATALOGUE,
    DEFAULT_RST,
    DEFAULT_STUB,
    _parse_doxygen,
    collect_authoring_api,
    collect_inventory,
)


_EXTENSION_MODULES = ("hgraph_persistence", "hgraph_web", "hgraph_kafka", "hgraph_analytics")
_core_inventory_cache = None


def collect_core_inventory():
    """collect_inventory() from the core registry only.

    Installed extensions register additional overloads into the process-global
    native registry when another test imports them (hgraph-persistence adds
    ``replay_const`` and the durable record/replay shapes). The generated files
    document the core surface (RFC 0025), so collect the comparison baseline in
    a clean interpreter whenever an extension module is loaded here.
    """
    global _core_inventory_cache
    if not any(
        name == extension or name.startswith(extension + ".")
        for name in sys.modules
        for extension in _EXTENSION_MODULES
    ):
        return collect_inventory()
    if _core_inventory_cache is None:
        script = (
            "import pickle, sys\n"
            # Editable-install finders would shadow the propagated sys.path
            # with a different build's native module; drop them first.
            "sys.meta_path = [finder for finder in sys.meta_path"
            " if 'ScikitBuild' not in type(finder).__name__]\n"
            "from tools.api_inventory import collect_inventory\n"
            "sys.stdout.buffer.write(pickle.dumps(collect_inventory()))\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=DEFAULT_RST.parents[3],
            capture_output=True,
            # The baseline must read the same native module as this process,
            # just without the extension imports test ordering brought in.
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        )
        assert result.returncode == 0, result.stderr.decode()
        _core_inventory_cache = pickle.loads(result.stdout)
    return _core_inventory_cache


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
        "CLOCK",
        "CompoundScalar",
        "DebugContext",
        "EvaluationClock",
        "EvaluationEngineApi",
        "GlobalState",
        "Graph",
        "LOGGER",
        "NODE",
        "Node",
        "REF",
        "RECORDABLE_STATE",
        "RecordReplayContext",
        "SCHEDULER",
        "STATE",
        "TS_OUT",
        "Traits",
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
    inventory = collect_core_inventory()
    operator_names = {entry["name"] for entry in inventory["operators"]}

    assert "add_" in operator_names
    assert "add_" not in hgraph.__all__
    assert callable(hgraph.add_)
    assert "add_:" in DEFAULT_STUB.read_text(encoding="utf-8")


def test_registered_override_kernels_are_grouped_under_public_operators():
    inventory = collect_core_inventory()
    operators = {entry["name"]: entry for entry in inventory["operators"]}

    assert OPERATOR_OVERRIDE_NAMES.isdisjoint(operators)
    assert OPERATOR_OVERRIDE_NAMES.isdisjoint(dir(hgraph))
    assert all(not hasattr(hgraph, name) for name in OPERATOR_OVERRIDE_NAMES)
    assert OPERATOR_OVERRIDE_NAMES <= set(_hgraph.operator_names())
    for public_name, expected_groups in OPERATOR_OVERRIDE_GROUPS.items():
        actual_groups = operators[public_name]["grouped_overrides"]
        assert tuple(
            (group["group_label"], group["name"]) for group in actual_groups
        ) == expected_groups


def test_operator_inventory_preserves_complete_native_overloads():
    inventory = collect_core_inventory()
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


def test_public_operator_patterns_use_python_generic_names_by_kind():
    formatter = PublicTypePatternFormatter()

    assert formatter.format("TS[~S]", category="time_series") == "TS[SCALAR]"
    assert formatter.format("TSL[~T, 0]", category="time_series") == \
        "TSL[TIME_SERIES_TYPE, SIZE]"
    assert formatter.format("TSD[~K, ~V]", category="time_series") == "TSD[K, V]"
    assert formatter.format("~O", category="time_series") == "OUT"
    assert formatter.format("~RESULT", category="time_series", output=True) == "OUT"


def test_structured_doxygen_operator_documentation_is_preserved():
    documentation = _parse_doxygen(
        """
        Add two live values.

        The selected overload controls promotion.
        @param lhs Left-hand input.
        @param rhs Right-hand input.
        @return The promoted sum.
        @par Python example
        @code{.py}
        total = lhs + rhs
        @endcode
        """
    )

    assert documentation.description == (
        "Add two live values.\n\nThe selected overload controls promotion."
    )
    assert dict(documentation.parameters) == {
        "lhs": "Left-hand input.",
        "rhs": "Right-hand input.",
    }
    assert documentation.returns == "The promoted sum."
    assert documentation.examples == ("total = lhs + rhs",)


def test_operator_catalogue_exposes_every_operator_signature_and_documentation():
    inventory = collect_core_inventory()
    source = DEFAULT_OPERATOR_CATALOGUE.read_text(encoding="utf-8")

    assert source.count(".. _python-operator-") == len(inventory["operators"])
    assert "add_(lhs: TS[int], rhs: TS[int]) -> TS[int]" in source
    assert "abs_(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> OUT" in source
    assert "abs_(ts: TIME_SERIES_TYPE) -> OUT" in source
    assert "add_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]" in source
    assert "const(value: SCALAR) -> OUT" in source
    assert "Grouped overrides" in source
    assert "**Compound-scalar values**" in source
    assert "(ts: TIME_SERIES_TYPE, __strict__: bool) -> OUT" in source
    assert ".. _python-operator-combine_cs:" not in source
    assert "``combine_cs``" not in source
    assert ".. _python-operator-to_window:" in source
    assert "Accepted native overloads" in source
    assert source.count("\nReturns\n~~~~~~~\n") == len(inventory["operators"])
    assert source.count("\nPython example\n~~~~~~~~~~~~~~\n") == len(inventory["operators"])
    assert source.count("\nParameters\n~~~~~~~~~~\n") >= len(inventory["operators"]) - 1
    assert "Left-hand value. A tick triggers a new result" in source
    assert "total = lhs + rhs  # equivalent to hg.add_(lhs, rhs)" in source
    assert "When true, wait until both operands are valid" in source
    assert "``var_`` —" not in source
    assert "``zero_`` —" not in source
    assert "``union_`` —" not in source
    assert "running_variance = hg.var(returns)" not in source
    assert "additive_identity = hg.zero[TS[int]](hg.add_)" in source
    assert "all_symbols = hg.union(primary_symbols, secondary_symbols)" in source
    assert "latest source value if the source changed while the gate was closed" in source
    assert "condition reopens" in source
    assert "hg.until_true(lambda value: value >= target, price)" in source
    assert "@param" not in source
    assert "@code" not in source
    assert not any(
        re.search(r"~[A-Za-z_]", line)
        for line in source.splitlines() if " -> " in line
    )
    assert ", 0]" not in source
    assert "__out__" not in source
    signature_lines = "\n".join(
        line for line in source.splitlines()
        if line.startswith("   ") and "(" in line and " -> " in line
    )
    # No UNRESOLVED single-letter type var (``-> O``, ``: S``, ``[T]``) reached
    # the published signatures. The bound must be a whole identifier: the
    # earlier ``(?<![A-Z_])`` form only excluded an adjacent UPPERCASE letter,
    # so it also matched the lone capital inside an ordinary mixed-case name
    # (``RecordAsOf``). Every genuine bare type var is still caught - in
    # ``TS[int]`` or ``OUT`` the letter has a letter beside it either way.
    assert not re.search(r"(?<![A-Za-z0-9_])(?:O|S|T)(?![A-Za-z0-9_])", signature_lines)
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
    assert "Parameters\n~~~~~~~~~~" in ast.get_docstring(add)
    assert "Left-hand value. A tick triggers a new result" in ast.get_docstring(add)
    assert "total = lhs + rhs" in ast.get_docstring(add)

    abs_operator = classes["_abs__Operator"]
    abs_documentation = ast.get_docstring(abs_operator)
    assert "abs_(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> OUT" in abs_documentation
    assert "abs_(ts: TIME_SERIES_TYPE) -> OUT" in abs_documentation
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

    record = classes["_record_Operator"]
    # The durable overload (mode: _ToTableMode et al.) registers from
    # hgraph-persistence (RFC 0025); core's stub carries the in-memory
    # shapes, including the sparse/dense selection.
    assert any(
        any(
            argument.arg == "sparse"
            for argument in call.args.args + call.args.kwonlyargs
        )
        for call in record.body
        if isinstance(call, ast.FunctionDef) and call.name == "__call__"
    )
    to_table = classes["_to_table_Operator"]
    assert any(
        argument.arg == "mode"
        and ast.unparse(argument.annotation) == "_WiringPort | _ToTableMode"
        for call in to_table.body
        if isinstance(call, ast.FunctionDef) and call.name == "__call__"
        for argument in call.args.args + call.args.kwonlyargs
    )

    typing_all = next(
        node for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "__all__"
                for target in node.targets)
    )
    typed_lazy_names = {element.value for element in typing_all.value.elts}
    # The core inventory already folds override kernels and drops namespaced
    # extension operators (typed by their extensions, not by hgraph's stub) —
    # and, unlike the live registry, it is immune to extension-registered
    # operators such as hgraph-persistence's replay_const.
    public_registry_names = {
        entry["name"] for entry in collect_core_inventory()["operators"]
    }
    explicitly_typed_names = public_registry_names & set(hgraph.__all__)
    assert typed_lazy_names | explicitly_typed_names == public_registry_names
    assert typed_lazy_names.isdisjoint(explicitly_typed_names)


def test_typed_package_marker_is_present():
    marker = Path(hgraph.__file__).with_name("py.typed")
    assert marker.is_file()
