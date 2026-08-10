#!/usr/bin/env python3
"""Generate the checked Python API inventory, docs, and lazy-operator stub.

Run this with an installed hgraph wheel (or a built extension on
``PYTHONPATH``). The root package, native operator registry, and public module
``__all__`` declarations are deliberately separate inputs: none of them is a
complete API description by itself.
"""

from __future__ import annotations

import argparse
import ast
import importlib
import inspect
import keyword
import re
from pathlib import Path
from typing import Any

from hgraph._operator_groups import OPERATOR_OVERRIDE_GROUPS
from hgraph._operator_signature import PublicTypePatternFormatter


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RST = ROOT / "docs/source/reference/python_api_inventory.rst"
DEFAULT_OPERATOR_CATALOGUE = ROOT / "docs/source/reference/operator_catalogue.rst"
DEFAULT_AUTHORING_API = ROOT / "docs/source/reference/authoring_api.rst"
DEFAULT_STUB = ROOT / "python/hgraph/_operator_typing.pyi"
DEFAULT_OPERATOR_DOCS = ROOT / "python/hgraph/_operator_docs.py"
OPERATOR_HEADERS = ROOT / "include/hgraph/lib/std/operators"

PUBLIC_MODULES = (
    "hgraph.temporal",
    "hgraph.nodes",
    "hgraph.test",
    "hgraph.debug",
    "hgraph.stream",
    "hgraph.reflection",
    "hgraph.numpy_",
    "hgraph.arrow",
    "hgraph.notebook",
    "hgraph.adaptors.data_catalogue",
    "hgraph.adaptors.data_frame",
    "hgraph.adaptors.dataclass",
    "hgraph.adaptors.delta",
    "hgraph.adaptors.executor",
    "hgraph.adaptors.json",
    "hgraph.adaptors.perspective",
    "hgraph.adaptors.run_graph_on_thread",
    "hgraph.adaptors.sql",
    "hgraph.adaptors.tornado",
)

AUTHORING_API = (
    ("Authoring decorators", (
        ("hgraph", "graph"),
        ("hgraph", "compute_node"),
        ("hgraph", "sink_node"),
        ("hgraph", "generator"),
        ("hgraph", "push_queue"),
        ("hgraph", "operator"),
        ("hgraph", "dispatch"),
        ("hgraph", "component"),
    )),
    ("Execution and testing", (
        ("hgraph", "run_graph"),
        ("hgraph", "evaluate_graph"),
        ("hgraph", "GraphConfiguration"),
        ("hgraph.test", "eval_node"),
    )),
    ("Services and adaptors", (
        ("hgraph", "reference_service"),
        ("hgraph", "subscription_service"),
        ("hgraph", "request_reply_service"),
        ("hgraph", "service_impl"),
        ("hgraph", "register_service"),
        ("hgraph", "adaptor"),
        ("hgraph", "adaptor_impl"),
        ("hgraph", "service_adaptor"),
        ("hgraph", "service_adaptor_impl"),
        ("hgraph", "register_adaptor"),
    )),
)

_DOCUMENTED_OPERATOR = re.compile(
    r'/\*\*(?P<documentation>(?:(?!\*/).)*)\*/\s*'
    r'struct\s+\w+\s*:\s*Operator\s*<\s*"(?P<name>[^"]+)"',
    re.DOTALL,
)


def _clean_doxygen(documentation: str) -> str:
    lines = []
    for raw_line in documentation.splitlines():
        line = re.sub(r"^\s*\* ?", "", raw_line).rstrip()
        lines.append(line)
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()

    paragraphs = []
    current = []
    for line in lines:
        if line:
            current.append(line.strip())
        elif current:
            paragraphs.append(" ".join(current))
            current = []
    if current:
        paragraphs.append(" ".join(current))
    return "\n\n".join(paragraphs)


def collect_operator_documentation() -> dict[str, str]:
    """Read semantic summaries from the public C++ operator declarations."""
    collected: dict[str, list[str]] = {}
    for header in sorted(OPERATOR_HEADERS.glob("*.h")):
        source = header.read_text(encoding="utf-8")
        for match in _DOCUMENTED_OPERATOR.finditer(source):
            documentation = _clean_doxygen(match.group("documentation"))
            if documentation:
                entries = collected.setdefault(match.group("name"), [])
                if documentation not in entries:
                    entries.append(documentation)
    return {name: "\n\n".join(entries) for name, entries in collected.items()}


def _literal_all(module_name: str) -> tuple[str, ...]:
    """Read a literal ``__all__`` without importing an optional adaptor."""
    relative = Path(*module_name.split("."))
    if relative.parts[0] != "hgraph":
        return ()
    module_path = ROOT / "python" / relative
    source = module_path / "__init__.py"
    if not source.exists():
        source = module_path.with_suffix(".py")
    if not source.exists():
        return ()
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    for statement in tree.body:
        if not isinstance(statement, (ast.Assign, ast.AnnAssign)):
            continue
        targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
        if not any(isinstance(target, ast.Name) and target.id == "__all__" for target in targets):
            continue
        try:
            value = ast.literal_eval(statement.value)
        except (TypeError, ValueError):
            return ()
        if isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value):
            return tuple(value)
    return ()


def module_exports(module_name: str) -> tuple[str, ...]:
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        # Optional adaptors should still appear in the inventory generated by
        # a core-only environment. Their package __all__ is intentionally a
        # literal so it can be read without importing the optional runtime.
        return _literal_all(module_name)
    return tuple(getattr(module, "__all__", ()))


def _signature_text(value: Any) -> str:
    """Return an inspect signature with process-specific sentinel reprs hidden."""
    signature = str(inspect.signature(value))
    return re.sub(r"<object object at 0x[0-9a-fA-F]+>", "...", signature)


def collect_inventory() -> dict[str, Any]:
    import _hgraph
    import hgraph

    operator_documentation = collect_operator_documentation()
    registry_operators: dict[str, dict[str, Any]] = {}
    for name in sorted(_hgraph.operator_names()):
        if name.startswith("__"):
            continue
        overloads = []
        for raw_overload in _hgraph.operator_overload_signatures(name):
            (raw_parameters, variadic, positional_params, has_kwargs,
             kwargs_pattern, has_output, output_pattern) = raw_overload
            parameters = tuple({
                "name": parameter_name,
                "kind": "time-series" if is_time_series else "scalar",
                "type_pattern": type_pattern,
                "has_default": bool(has_default),
            } for parameter_name, is_time_series, type_pattern, has_default in raw_parameters)
            overloads.append({
                "parameters": parameters,
                "variadic": bool(variadic),
                "positional_params": int(positional_params),
                "has_kwargs": bool(has_kwargs),
                "kwargs_pattern": kwargs_pattern,
                "has_output": bool(has_output),
                "output_pattern": output_pattern,
            })
        explicit_root = name in hgraph.__all__
        python_signature = None
        if explicit_root:
            try:
                python_signature = _signature_text(getattr(hgraph, name))
            except (TypeError, ValueError):
                pass
        registry_operators[name] = {
            "name": name,
            "overloads": tuple(overloads),
            "documentation": operator_documentation.get(name),
            # Explicit top-level helpers already carry their own annotations.
            # Generated declarations must not replace those richer contracts
            # inside hgraph.__init__'s TYPE_CHECKING import.
            "explicit_root": explicit_root,
            "python_signature": python_signature,
            "grouped_overrides": (),
        }

    for public_name, override_groups in OPERATOR_OVERRIDE_GROUPS.items():
        public_operator = registry_operators.get(public_name)
        if public_operator is None:
            value = getattr(hgraph, public_name)
            public_operator = {
                "name": public_name,
                "overloads": (),
                "documentation": inspect.getdoc(value),
                "explicit_root": public_name in hgraph.__all__,
                "python_signature": _signature_text(value),
                "grouped_overrides": (),
            }
            registry_operators[public_name] = public_operator

        grouped_overrides = []
        for label, override_name in override_groups:
            override = registry_operators.pop(override_name)
            grouped_overrides.append({**override, "group_label": label})
        public_operator["grouped_overrides"] = tuple(grouped_overrides)

    operators = tuple(
        registry_operators[name] for name in sorted(registry_operators)
    )

    modules = {
        module_name: tuple(sorted(dict.fromkeys(module_exports(module_name))))
        for module_name in PUBLIC_MODULES
    }
    return {
        "root": tuple(sorted(dict.fromkeys(hgraph.__all__))),
        "operators": operators,
        "modules": modules,
    }


def collect_authoring_api() -> tuple[dict[str, Any], ...]:
    """Collect curated Python authoring callables from their public modules."""
    groups = []
    for heading, entries in AUTHORING_API:
        callables = []
        for module_name, name in entries:
            module = importlib.import_module(module_name)
            value = getattr(module, name)
            try:
                signature = _signature_text(value)
                parameters = tuple(inspect.signature(value).parameters)
            except (TypeError, ValueError):
                signature = "(*args, **kwargs)"
                parameters = ("args", "kwargs")
            callables.append({
                "name": name,
                "qualified_name": f"{module_name}.{name}",
                "signature": signature,
                "parameters": parameters,
                "documentation": inspect.getdoc(value) or "",
                "kind": "class" if inspect.isclass(value) else "function",
            })
        groups.append({"heading": heading, "callables": tuple(callables)})
    return tuple(groups)


def _display_signature(operator: dict[str, Any]) -> str:
    if operator["grouped_overrides"]:
        group_count = len(operator["grouped_overrides"]) + bool(
            _display_overload_signatures(operator)
        )
        return f"{group_count} overload group{'s' if group_count != 1 else ''}"
    signatures = _display_overload_signatures(operator)
    if not signatures:
        return f"{operator['name']}(*args, **kwargs)"
    if len(signatures) == 1:
        return signatures[0]
    return f"{len(signatures)} overloads"


def _hlist(names: tuple[str, ...], indent: str = "") -> list[str]:
    if not names:
        return [f"{indent}No separately exported names."]
    lines = [f"{indent}.. hlist::", f"{indent}   :columns: 3", ""]
    lines.extend(f"{indent}   * - ``{name}``" for name in names)
    return lines


def render_rst(inventory: dict[str, Any]) -> str:
    operators = inventory["operators"]
    modules = inventory["modules"]
    lines = [
        "Python API inventory",
        "====================",
        "",
        ".. This file is generated by tools/api_inventory.py. Do not edit it by hand.",
        "",
        "This inventory is generated from the installed ``hgraph`` package and",
        "native operator registry. It complements the curated reference pages:",
        "``hgraph.__all__`` describes wildcard imports, while the operator registry",
        "also supplies lazy module attributes such as ``add_`` and ``filter_``.",
        "Names beginning with two underscores are runtime implementation entries and",
        "are omitted. Separately registered override kernels are grouped beneath the",
        "public operator they implement rather than listed as top-level operators.",
        "",
        ".. list-table:: Inventory summary",
        "   :header-rows: 1",
        "   :widths: 55 15",
        "",
        "   * - Surface",
        "     - Names",
        f"   * - ``hgraph.__all__``",
        f"     - {len(inventory['root'])}",
        "   * - Public operator groups",
        f"     - {len(operators)}",
        "   * - Public submodules",
        f"     - {len(modules)}",
        "",
        "Top-level wildcard exports",
        "--------------------------",
        "",
    ]
    lines.extend(_hlist(inventory["root"]))
    lines.extend([
        "",
        "Operator registry",
        "-----------------",
        "",
        "The call shapes below come from the complete native overload metadata.",
        "The generated typing declarations and runtime operator docstrings list the",
        "individual accepted signatures, including defaults and keyword-only inputs.",
        "Generic names use the public Python vocabulary: ``SCALAR`` for scalar",
        "payloads, ``TIME_SERIES_TYPE`` for complete time-series types, ``SIZE``",
        "for a fixed TSL length, and ``OUT`` for an inferred output.",
        "The coverage column distinguishes lazy proxies from explicit Python helpers",
        "whose curated signatures remain authoritative.",
        "",
        ".. list-table::",
        "   :header-rows: 1",
        "   :widths: 25 55 20",
        "",
        "   * - Name",
        "     - Registry call shape",
        "     - Coverage",
    ])
    for operator in operators:
        overload_count = len(_display_overload_signatures(operator)) + sum(
            len(_display_overload_signatures(override, name=operator["name"]))
            for override in operator["grouped_overrides"]
        )
        exposure = "explicit helper" if operator["explicit_root"] else "lazy operator"
        group_count = len(operator["grouped_overrides"]) + bool(
            _display_overload_signatures(operator)
        )
        grouping = (
            f" across {group_count} group{'s' if group_count != 1 else ''}"
            if operator["grouped_overrides"] else ""
        )
        lines.extend([
            f"   * - :ref:`{operator['name']} <python-operator-{operator['name']}>`",
            f"     - ``{_display_signature(operator)}``",
            f"     - {overload_count} native overload{'s' if overload_count != 1 else ''}{grouping}; {exposure}",
        ])

    lines.extend(["", "Public submodules", "-----------------", ""])
    for module_name, exports in modules.items():
        lines.extend([module_name, "~" * len(module_name), ""])
        lines.extend(_hlist(exports))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_operator_catalogue(inventory: dict[str, Any]) -> str:
    """Render every public operator and its exact native overloads."""
    lines = [
        "Operator catalogue",
        "==================",
        "",
        ".. This file is generated by tools/api_inventory.py. Do not edit it by hand.",
        "",
        "This catalogue is generated from the native operator registry and the",
        "semantic documentation on the public C++ declarations. Each signature is",
        "an accepted native wiring overload. ``TS[...]`` parameters accept wiring",
        "ports and, where dispatch permits, compatible plain values that are lifted",
        "to constant sources. ``...`` marks a default supplied by the overload.",
        "Generic names use the public Python vocabulary: ``SCALAR`` for scalar",
        "payloads, ``TIME_SERIES_TYPE`` for complete time-series types, ``SIZE``",
        "for a fixed TSL length, and ``OUT`` for an inferred output. ``K`` and ``V``",
        "retain their conventional key/value relationships.",
        "",
        "Explicit helpers have a curated Python entry point in addition to their",
        "native overloads. Lazy operators are resolved from ``hgraph`` on first use.",
        "",
        ".. contents:: Operators",
        "   :local:",
        "   :depth: 1",
        "",
    ]
    for operator in inventory["operators"]:
        name = operator["name"]
        signatures = _display_overload_signatures(operator)
        lines.extend([
            f".. _python-operator-{name}:",
            "",
            f"``{name}``",
            "-" * (len(name) + 4),
            "",
            operator["documentation"] or (
                f"Wire ``{name}`` through native overload resolution."
            ),
            "",
        ])
        if operator["explicit_root"]:
            signature = operator["python_signature"] or "(*args, **kwargs)"
            lines.extend([
                f"Python entry point: ``{name}{signature}`` (explicit helper).",
                "",
            ])
        else:
            lines.extend([
                "Python exposure: lazy native operator proxy.",
                "",
            ])
        if signatures:
            lines.extend([
                "Accepted native overloads",
                "",
                ".. code-block:: text",
                "",
            ])
            lines.extend(f"   {signature}" for signature in signatures)
            lines.append("")
        elif not operator["grouped_overrides"]:
            lines.extend([
                "Accepted native overloads",
                "",
                ".. code-block:: text",
                "",
            ])
            lines.append(f"   {name}(*args, **kwargs)")
            lines.append("")
        if operator["grouped_overrides"]:
            lines.extend([
                "Grouped overrides",
                "~~~~~~~~~~~~~~~~~",
                "",
                f"These native implementation groups provide overloads of ``{name}``.",
                "Their registry dispatch names are intentionally not presented as",
                "separate Python operators.",
                "",
            ])
            for override in operator["grouped_overrides"]:
                lines.extend([
                    f"**{override['group_label']}**",
                    "",
                    override["documentation"] or "Native implementation grouping.",
                    "",
                    "Native grouping contracts:",
                    "",
                    ".. code-block:: text",
                    "",
                ])
                lines.extend(
                    f"   {signature}"
                    for signature in _display_overload_signatures(override, name="")
                )
                lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _indent_rst(documentation: str, prefix: str = "   ") -> list[str]:
    return [f"{prefix}{line}" if line else "" for line in documentation.splitlines()]


def render_authoring_api(groups: tuple[dict[str, Any], ...]) -> str:
    """Render public Python authoring signatures without Sphinx autodoc."""
    lines = [
        "Authoring API detail",
        "====================",
        "",
        ".. This file is generated by tools/api_inventory.py. Do not edit it by hand.",
        "",
        "These are the primary Python entry points for authoring, running and testing",
        "graphs. Signatures and descriptions are captured from the public package so",
        "the checked-in page can be built without importing the native extension.",
        "",
    ]
    for group in groups:
        heading = group["heading"]
        lines.extend([heading, "-" * len(heading), ""])
        for entry in group["callables"]:
            directive = "py:class" if entry["kind"] == "class" else "py:function"
            lines.extend([
                f".. {directive}:: {entry['qualified_name']}{entry['signature']}",
                "",
            ])
            if entry["documentation"]:
                lines.extend(_indent_rst(entry["documentation"]))
            else:
                lines.append("   No public description is available.")
            lines.extend(["", ""])
    return "\n".join(lines).rstrip() + "\n"


def _typing_parameter_name(name: str, used: set[str]) -> str | None:
    if not name:
        return None
    candidate = f"{name}_" if keyword.iskeyword(name) else name
    if not candidate.isidentifier() or candidate in used:
        return None
    used.add(candidate)
    return candidate


def _overload_key(overload: dict[str, Any]) -> tuple[Any, ...]:
    return (
        tuple((parameter["name"], parameter["kind"], parameter["type_pattern"],
               parameter["has_default"]) for parameter in overload["parameters"]),
        overload["variadic"],
        overload["positional_params"],
        overload["has_kwargs"],
        overload["kwargs_pattern"],
        overload["has_output"],
        overload["output_pattern"],
    )


def _unique_overloads(operator: dict[str, Any]) -> list[dict[str, Any]]:
    unique: dict[tuple[Any, ...], dict[str, Any]] = {}
    for overload in operator["overloads"]:
        unique.setdefault(_overload_key(overload), overload)
    return list(unique.values())


def _format_public_signature(name: str, overload: dict[str, Any]) -> str:
    parameters = list(overload["parameters"])
    formatter = PublicTypePatternFormatter()
    variadic_parameter = parameters.pop() if overload["variadic"] and parameters else None
    positional_count = min(overload["positional_params"], len(parameters))
    rendered = []
    for index, parameter in enumerate(parameters[:positional_count]):
        parameter_name = parameter["name"] or f"arg{index}"
        default = " = ..." if parameter["has_default"] else ""
        pattern = formatter.format(
            parameter["type_pattern"],
            category="time_series" if parameter["kind"] == "time-series" else "scalar",
        )
        rendered.append(f"{parameter_name}: {pattern}{default}")
    if variadic_parameter is not None:
        parameter_name = variadic_parameter["name"] or "args"
        pattern = formatter.format(
            variadic_parameter["type_pattern"],
            category=(
                "time_series" if variadic_parameter["kind"] == "time-series"
                else "scalar"
            ),
        )
        rendered.append(f"*{parameter_name}: {pattern}")
    elif positional_count < len(parameters):
        rendered.append("*")
    for index, parameter in enumerate(parameters[positional_count:], start=positional_count):
        parameter_name = parameter["name"] or f"arg{index}"
        default = " = ..." if parameter["has_default"] else ""
        pattern = formatter.format(
            parameter["type_pattern"],
            category="time_series" if parameter["kind"] == "time-series" else "scalar",
        )
        rendered.append(f"{parameter_name}: {pattern}{default}")
    if overload["has_kwargs"]:
        pattern = formatter.format(
            overload["kwargs_pattern"] or "time-series", category="time_series")
        rendered.append(f"**kwargs: {pattern}")
    output = (
        formatter.format(
            overload["output_pattern"], category="time_series", output=True)
        if overload["has_output"] else "None"
    )
    return f"{name}({', '.join(rendered)}) -> {output}"


def _display_overload_signatures(
        operator: dict[str, Any], *, name: str | None = None) -> tuple[str, ...]:
    """Return distinct public renderings while retaining raw overload metadata."""
    return tuple(dict.fromkeys(
        _format_public_signature(
            operator["name"] if name is None else name, overload
        )
        for overload in _unique_overloads(operator)
    ))


_SCALAR_ANNOTATIONS = {
    "bool": "bool",
    "int": "int",
    "float": "float",
    "str": "str",
    "bytes": "bytes",
    "date": "_date",
    "datetime": "_datetime",
    "time": "_time",
    "timedelta": "_timedelta",
    "ambiguous_time_policy": "_AmbiguousTimePolicy",
    "civil_date_range": "_CivilDateRange",
    "civil_datetime": "_CivilDateTime",
    "CmpResult": "_CmpResult",
    "DivideByZero": "_DivideByZero",
    "instant_range": "_InstantRange",
    "month_end_policy": "_MonthEndPolicy",
    "nonexistent_time_policy": "_NonexistentTimePolicy",
    "period": "_Period",
    "zone_id": "_ZoneId",
    "zoned_datetime": "_ZonedDateTime",
}


def _parameter_annotation(parameter: dict[str, Any]) -> str:
    pattern = parameter["type_pattern"]
    if parameter["kind"] == "scalar":
        if pattern in {"callable", "fn"}:
            return "_Callable[..., object]"
        return _SCALAR_ANNOTATIONS.get(pattern, "object")
    if pattern == "SIGNAL":
        return "_WiringPort"
    match = re.fullmatch(r"TS\[([^\[\],]+)\]", pattern)
    if match and match.group(1) in _SCALAR_ANNOTATIONS:
        return f"_WiringPort | {_SCALAR_ANNOTATIONS[match.group(1)]}"
    # Ports are not schema-generic in the Python bridge yet. Preserve the
    # exact native pattern in the docstring while accurately accepting both a
    # wired port and values that native dispatch may lift to const sources.
    return "_WiringPort | object"


def _typing_parameter(parameter: dict[str, Any], used: set[str], index: int) -> str:
    name = _typing_parameter_name(parameter["name"], used)
    if name is None:
        name = f"arg{index}"
        while name in used:
            index += 1
            name = f"arg{index}"
        used.add(name)
    default = " = ..." if parameter["has_default"] else ""
    return f"{name}: {_parameter_annotation(parameter)}{default}"


def _python_signature_variants(overload: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    """Represent native default-before-required calls with valid Python forms.

    Native dispatch permits ``(required, optional=..., required)`` because it
    normalises positional and named arguments itself. Python syntax expresses
    the same accepted calls as a fully positional form (the earlier default is
    required there) plus a form making the suffix keyword-only.
    """
    parameters = overload["parameters"]
    positional_count = min(overload["positional_params"],
                           len(parameters) - (1 if overload["variadic"] else 0))
    first_default = next((
        index for index, parameter in enumerate(parameters[:positional_count])
        if parameter["has_default"]
    ), None)
    if first_default is None or not any(
        not parameter["has_default"]
        for parameter in parameters[first_default + 1:positional_count]
    ):
        return (overload,)

    positional = dict(overload)
    positional["parameters"] = tuple(
        dict(parameter, has_default=False)
        if index < positional_count and parameter["has_default"]
        else parameter
        for index, parameter in enumerate(parameters)
    )
    keyword_suffix = dict(overload)
    keyword_suffix["positional_params"] = first_default
    return positional, keyword_suffix


def _render_stub_signature(overload: dict[str, Any]) -> str:
    parameters = list(overload["parameters"])
    variadic_parameter = parameters.pop() if overload["variadic"] and parameters else None
    positional_count = min(overload["positional_params"], len(parameters))
    used = {"self"}
    rendered = [
        _typing_parameter(parameter, used, index)
        for index, parameter in enumerate(parameters[:positional_count])
    ]
    if variadic_parameter is not None:
        name = _typing_parameter_name(variadic_parameter["name"], used) or "args"
        rendered.append(f"*{name}: {_parameter_annotation(variadic_parameter)}")
    elif positional_count < len(parameters):
        rendered.append("*")
    rendered.extend(
        _typing_parameter(parameter, used, positional_count + index)
        for index, parameter in enumerate(parameters[positional_count:])
    )
    if overload["has_kwargs"]:
        rendered.append("**kwargs: _WiringPort | object")
    return ", ".join(rendered)


def _operator_class_name(name: str) -> str:
    identifier = re.sub(r"\W", "_", name)
    return f"_{identifier}_Operator"


def render_operator_stub(inventory: dict[str, Any]) -> str:
    operators = [operator for operator in inventory["operators"] if not operator["explicit_root"]]
    lines = [
        '"""Generated typing declarations for operators exposed lazily by ``hgraph``.',
        "",
        "Each overload, default, variadic tail, keyword-only boundary, and output",
        "comes from native registry metadata. Regenerate with",
        '``tools/api_inventory.py``; runtime dispatch remains registry-owned."""',
        "",
        "from __future__ import annotations",
        '# mypy: disable-error-code="overload-cannot-match,overload-overlap"',
        "",
        "from datetime import (date as _date, datetime as _datetime, time as _time,",
        "                      timedelta as _timedelta)",
        "from typing import (Any as _Any, Callable as _Callable, Protocol as _Protocol,",
        "                    Self as _Self, overload as _overload)",
        "",
        "from _hgraph import (AmbiguousTimePolicy as _AmbiguousTimePolicy,",
        "                     CivilDateRange as _CivilDateRange,",
        "                     CivilDateTime as _CivilDateTime, InstantRange as _InstantRange,",
        "                     MonthEndPolicy as _MonthEndPolicy,",
        "                     NonexistentTimePolicy as _NonexistentTimePolicy,",
        "                     Period as _Period, ZoneId as _ZoneId,",
        "                     ZonedDateTime as _ZonedDateTime)",
        "from ._compat import CmpResult as _CmpResult, DivideByZero as _DivideByZero",
        "from ._wiring import WiringPort as _WiringPort",
        "",
    ]
    for operator in operators:
        class_name = _operator_class_name(operator["name"])
        overloads = _unique_overloads(operator)
        documentation = (
            operator["documentation"]
            or f'Wire ``{operator["name"]}`` through native overload resolution.'
        ).splitlines()
        lines.extend([
            f"class {class_name}(_Protocol):",
            '    """' + documentation[0],
        ])
        lines.extend(f"    {line}" if line else "" for line in documentation[1:])
        lines.extend([
            "",
            "    Accepted native overloads:",
            "",
        ])
        for signature in _display_overload_signatures(operator):
            lines.append(f"    - ``{signature}``")
        lines.extend([
            "",
            "    Time-series parameters accept wiring ports and compatible plain",
            "    values that can be lifted to constant sources. Generic names use",
            "    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,",
            '    ``SIZE``, ``OUT``, ``K`` and ``V``."""',
            "",
        ])
        stub_overloads = []
        seen_stub_signatures = set()
        for overload_signature in overloads:
            for variant in _python_signature_variants(overload_signature):
                rendered = _render_stub_signature(variant)
                output = "_WiringPort" if variant["has_output"] else "None"
                key = rendered, output
                if key not in seen_stub_signatures:
                    seen_stub_signatures.add(key)
                    stub_overloads.append((rendered, output))
        if not stub_overloads:
            lines.append("    def __call__(self, *args: object, **kwargs: object) -> _WiringPort | None: ...")
        else:
            for parameters, output in stub_overloads:
                if len(stub_overloads) > 1:
                    lines.append("    @_overload")
                separator = ", " if parameters else ""
                lines.append(
                    f"    def __call__(self{separator}{parameters}) -> {output}: ..."
                )
        lines.extend([
            "    def __getitem__(self, item: _Any, /) -> _Self: ...",
            "",
            f"{operator['name']}: {class_name}",
            "",
        ])
    lines.extend(["", "__all__ = ("])
    lines.extend(f'    "{operator["name"]}",' for operator in operators)
    lines.extend([")", ""])
    return "\n".join(lines)


def render_operator_docs(inventory: dict[str, Any]) -> str:
    lines = [
        '"""Generated semantic documentation for native operator proxies.',
        "",
        "Source summaries live on the public C++ operator declarations; exact",
        'accepted signatures come from the runtime registry."""',
        "",
        "OPERATOR_DOCS = {",
    ]
    for operator in inventory["operators"]:
        if operator["documentation"]:
            lines.append(f"    {operator['name']!r}: {operator['documentation']!r},")
    lines.extend(["}", ""])
    return "\n".join(lines)


def _write_or_check(path: Path, content: str, check: bool) -> bool:
    if check:
        return path.exists() and path.read_text(encoding="utf-8") == content
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rst", type=Path, default=DEFAULT_RST)
    parser.add_argument("--operator-catalogue", type=Path, default=DEFAULT_OPERATOR_CATALOGUE)
    parser.add_argument("--authoring-api", type=Path, default=DEFAULT_AUTHORING_API)
    parser.add_argument("--stub", type=Path, default=DEFAULT_STUB)
    parser.add_argument("--operator-docs", type=Path, default=DEFAULT_OPERATOR_DOCS)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()

    inventory = collect_inventory()
    authoring_api = collect_authoring_api()
    outputs = {
        arguments.rst: render_rst(inventory),
        arguments.operator_catalogue: render_operator_catalogue(inventory),
        arguments.authoring_api: render_authoring_api(authoring_api),
        arguments.stub: render_operator_stub(inventory),
        arguments.operator_docs: render_operator_docs(inventory),
    }
    stale = [path for path, content in outputs.items() if not _write_or_check(path, content, arguments.check)]
    if stale:
        for path in stale:
            print(f"stale generated API file: {path}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
