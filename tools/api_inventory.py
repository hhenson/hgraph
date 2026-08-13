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
from dataclasses import dataclass
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


@dataclass(frozen=True)
class OperatorDocumentation:
    """Structured documentation recovered from one public C++ declaration."""

    description: str
    parameters: tuple[tuple[str, str], ...] = ()
    returns: str | None = None
    examples: tuple[str, ...] = ()


def _doxygen_lines(documentation: str) -> list[str]:
    lines = []
    for raw_line in documentation.splitlines():
        line = re.sub(r"^\s*\* ?", "", raw_line).rstrip()
        lines.append(line)
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    if lines:
        lines[0] = lines[0].lstrip()
    continuation_indents = [
        len(line) - len(line.lstrip())
        for line in lines[1:] if line
    ]
    base_indent = min(continuation_indents, default=0)
    if base_indent:
        lines[1:] = [
            line[base_indent:] if line else line for line in lines[1:]
        ]
    return lines


def _join_prose(lines: list[str]) -> str:
    """Join wrapped Doxygen prose while preserving paragraph boundaries."""

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


def _parse_doxygen(documentation: str) -> OperatorDocumentation:
    """Parse the small, standard Doxygen subset used by operator declarations.

    ``@param`` and ``@return`` carry behavioural reference text. A Python
    example uses Doxygen's ``@par Python example`` followed by a
    ``@code{.py}`` / ``@endcode`` block. Untagged comments remain valid and
    become the operator description.
    """
    description: list[str] = []
    parameters: dict[str, list[str]] = {}
    returns: list[str] = []
    examples: list[str] = []
    active: tuple[str, str | None] = ("description", None)
    code: list[str] | None = None

    def append(line: str) -> None:
        section, name = active
        if section == "description":
            description.append(line)
        elif section == "parameter" and name is not None:
            parameters[name].append(line)
        elif section == "returns":
            returns.append(line)

    for line in _doxygen_lines(documentation):
        if line.startswith("@param "):
            match = re.match(r"@param\s+(?:\[[^]]+\]\s*)?(\S+)\s*(.*)", line)
            if match is None:
                raise ValueError(f"invalid operator @param tag: {line}")
            name, text = match.groups()
            parameters.setdefault(name, [])
            active = ("parameter", name)
            if text:
                parameters[name].append(text)
            continue
        if line.startswith(("@return ", "@returns ")):
            text = line.split(maxsplit=1)[1]
            active = ("returns", None)
            returns.append(text)
            continue
        if line == "@par Python example":
            active = ("example", None)
            continue
        if line.startswith("@code"):
            code = []
            continue
        if line == "@endcode":
            if code is None:
                raise ValueError("operator documentation has @endcode without @code")
            example = "\n".join(code).strip("\n")
            if example:
                examples.append(example)
            code = None
            active = ("description", None)
            continue
        if code is not None:
            code.append(line)
        elif active[0] != "example":
            append(line)

    if code is not None:
        raise ValueError("operator documentation has an unterminated @code block")
    return OperatorDocumentation(
        description=_join_prose(description),
        parameters=tuple(
            (name, _join_prose(lines)) for name, lines in parameters.items()
        ),
        returns=_join_prose(returns) or None,
        examples=tuple(examples),
    )


def _combine_documentation(
        entries: list[OperatorDocumentation]) -> OperatorDocumentation:
    descriptions = tuple(dict.fromkeys(
        entry.description for entry in entries if entry.description
    ))
    parameters: dict[str, str] = {}
    returns = []
    examples = []
    for entry in entries:
        for name, documentation in entry.parameters:
            parameters.setdefault(name, documentation)
        if entry.returns and entry.returns not in returns:
            returns.append(entry.returns)
        for example in entry.examples:
            if example not in examples:
                examples.append(example)
    return OperatorDocumentation(
        description="\n\n".join(descriptions),
        parameters=tuple(parameters.items()),
        returns="\n\n".join(returns) or None,
        examples=tuple(examples),
    )


def _parse_python_documentation(documentation: str) -> OperatorDocumentation:
    """Parse Sphinx-style fields and an ``Example::`` block from a helper docstring."""
    description = []
    parameters: dict[str, list[str]] = {}
    returns = []
    example = []
    active: tuple[str, str | None] = ("description", None)
    in_example = False
    for line in documentation.splitlines():
        stripped = line.strip()
        parameter = re.match(r":param\s+(\S+):\s*(.*)", stripped)
        if parameter:
            name, text = parameter.groups()
            parameters.setdefault(name, [])
            active = ("parameter", name)
            if text:
                parameters[name].append(text)
            in_example = False
            continue
        if stripped.startswith(":return:"):
            active = ("returns", None)
            text = stripped.removeprefix(":return:").strip()
            if text:
                returns.append(text)
            in_example = False
            continue
        if stripped == "Example::":
            active = ("example", None)
            in_example = True
            continue
        if in_example:
            if not stripped and not example:
                continue
            if not line.startswith("    ") and stripped:
                in_example = False
                active = ("description", None)
            else:
                example.append(line[4:] if line.startswith("    ") else "")
                continue

        section, name = active
        if section == "description":
            description.append(line)
        elif section == "parameter" and name is not None:
            parameters[name].append(stripped)
        elif section == "returns":
            returns.append(stripped)

    rendered_example = "\n".join(example).strip("\n")
    return OperatorDocumentation(
        description=_join_prose(description),
        parameters=tuple(
            (name, _join_prose(lines)) for name, lines in parameters.items()
        ),
        returns=_join_prose(returns) or None,
        examples=(rendered_example,) if rendered_example else (),
    )


def collect_operator_documentation() -> dict[str, OperatorDocumentation]:
    """Read semantic reference material from public C++ declarations."""
    collected: dict[str, list[OperatorDocumentation]] = {}
    for header in sorted(OPERATOR_HEADERS.glob("*.h")):
        source = header.read_text(encoding="utf-8")
        for match in _DOCUMENTED_OPERATOR.finditer(source):
            documentation = _parse_doxygen(match.group("documentation"))
            if documentation.description:
                entries = collected.setdefault(match.group("name"), [])
                if documentation not in entries:
                    entries.append(documentation)
    return {
        name: _combine_documentation(entries)
        for name, entries in collected.items()
    }


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
    """Return an inspect signature using stable public type-variable names."""
    signature = str(inspect.signature(value))
    signature = re.sub(r"<object object at 0x[0-9a-fA-F]+>", "...", signature)
    # ``typing.TypeVar.__repr__`` prefixes invariant variables with ``~``.
    # That is an implementation marker, not part of the public hgraph type
    # vocabulary used by the catalogue and generated signatures.
    return re.sub(r"~(?=[A-Za-z_][A-Za-z0-9_]*)", "", signature)


def collect_inventory() -> dict[str, Any]:
    import _hgraph
    import hgraph

    operator_documentation = collect_operator_documentation()
    registry_operators: dict[str, dict[str, Any]] = {}
    for name in sorted(_hgraph.operator_names()):
        if name.startswith("__"):
            continue
        semantic_documentation = operator_documentation.get(name)
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
        python_parameters = ()
        if explicit_root:
            try:
                value = getattr(hgraph, name)
                signature = inspect.signature(value)
                python_signature = _signature_text(value)
                python_parameters = tuple({
                    "name": parameter.name,
                    "kind": parameter.kind.name,
                    "has_default": parameter.default is not inspect.Parameter.empty,
                } for parameter in signature.parameters.values())
            except (TypeError, ValueError):
                pass
        registry_operators[name] = {
            "name": name,
            "overloads": tuple(overloads),
            "documentation": (
                semantic_documentation.description
                if semantic_documentation is not None else None
            ),
            "parameter_documentation": dict(
                semantic_documentation.parameters
                if semantic_documentation is not None else ()
            ),
            "return_documentation": (
                semantic_documentation.returns
                if semantic_documentation is not None else None
            ),
            "examples": (
                semantic_documentation.examples
                if semantic_documentation is not None else ()
            ),
            # Explicit top-level helpers already carry their own annotations.
            # Generated declarations must not replace those richer contracts
            # inside hgraph.__init__'s TYPE_CHECKING import.
            "explicit_root": explicit_root,
            "python_signature": python_signature,
            "python_parameters": python_parameters,
            "grouped_overrides": (),
        }

    for public_name, override_groups in OPERATOR_OVERRIDE_GROUPS.items():
        public_operator = registry_operators.get(public_name)
        if public_operator is None:
            value = getattr(hgraph, public_name)
            helper_documentation = _parse_python_documentation(
                inspect.getdoc(value) or "")
            try:
                signature = inspect.signature(value)
                python_signature = _signature_text(value)
                python_parameters = tuple({
                    "name": parameter.name,
                    "kind": parameter.kind.name,
                    "has_default": parameter.default is not inspect.Parameter.empty,
                } for parameter in signature.parameters.values())
            except (TypeError, ValueError):
                python_signature = "(*args, **kwargs)"
                python_parameters = ()
            public_operator = {
                "name": public_name,
                "overloads": (),
                "documentation": helper_documentation.description,
                "parameter_documentation": dict(helper_documentation.parameters),
                "return_documentation": helper_documentation.returns,
                "examples": helper_documentation.examples,
                "explicit_root": public_name in hgraph.__all__,
                "python_signature": python_signature,
                "python_parameters": python_parameters,
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
        "Examples assume ``import hgraph as hg``. Names such as ``price`` or ``ts``",
        "represent wiring ports inside a graph; compatible literals may be lifted to",
        "constant sources. The examples emphasize normal Python authoring rather than",
        "the equivalent C++ ``wire`` spelling.",
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
        lines.extend([
            _operator_semantic_documentation(operator, include_description=False),
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


_COMMON_PARAMETER_DOCUMENTATION = {
    "ts": "The primary time-series input.",
    "ts1": "The first time-series input.",
    "ts2": "The second time-series input.",
    "tsl": "The collection or variadic sequence of time-series inputs.",
    "tsd": "The keyed time-series dictionary input.",
    "tsw": "The time-series window input.",
    "lhs": "The left-hand operand.",
    "rhs": "The right-hand operand.",
    "args": "Additional positional time-series inputs.",
    "kwargs": "Additional named time-series inputs.",
    "condition": "Boolean time series that controls whether the operation is active.",
    "predicate": "Value or time series used to decide which values are selected.",
    "signal": "Trigger signal; only its tick timing is significant.",
    "reset": "Optional signal that clears the operator's accumulated state when it ticks.",
    "value": "Value used to construct or update the output.",
    "values": "Values used to construct or update the output.",
    "key": "Key selecting the affected entry.",
    "keys": "Keys associated with the supplied values.",
    "item": "Item to locate or test.",
    "index": "Index selecting the requested item or output.",
    "idx": "Index selecting the requested item.",
    "default": "Fallback used when no more specific value is available.",
    "default_value": "Value emitted when the primary input has no usable value.",
    "period": "Tick count or elapsed interval controlling the temporal operation.",
    "delay": "Delay applied before scheduling or releasing a value.",
    "count": "Number of ticks or values affected by the operation.",
    "buffer_length": "Maximum number of values retained while buffering.",
    "min_window_period": "Minimum populated window size required before the output becomes valid.",
    "start": "Inclusive starting position or boundary.",
    "stop": "Exclusive stopping position or boundary.",
    "step_size": "Stride between forwarded input ticks.",
    "fn": "Callable invoked by the operator.",
    "func": "Graph or callable applied by the operator.",
    "expr": "Predicate expression applied to each candidate value.",
    "op": "Operator whose identity or zero value is requested.",
    "__strict__": (
        "Validity policy. When true, all required inputs must be valid; "
        "non-strict overloads may use the valid subset."
    ),
    "divide_by_zero": "Policy controlling the result when the divisor is zero.",
    "tick_once_only": "When true, passivates after the first qualifying tick.",
    "disjoint": (
        "When true, selects the faster path that assumes keyed inputs do not "
        "contain overlapping keys."
    ),
    "unique": "Controls whether duplicate values are represented by one key or a set of keys.",
    "ddof": "Delta degrees of freedom subtracted from the sample count in the divisor.",
    "alpha": "Exponential smoothing factor; larger values weight recent ticks more strongly.",
    "n_digits": "Number of digits retained after the decimal point.",
    "separator": "String inserted between items or used to divide the input.",
    "fmt": "Python-style format string applied to the supplied values.",
    "__sample__": "Emit only every nth formatted value; one emits every value.",
    "attr": "Attribute name selected at wiring time.",
    "error_msg": "Message used when the assertion fails.",
    "label": "Diagnostic label prefixed to emitted output.",
    "level": "Logging severity used for the formatted message.",
    "recordable_id": "Stable identifier used to locate recorded data.",
    "zone": "Time zone used to interpret or display the temporal value.",
    "month_end_policy": "Policy for dates whose day does not exist in the target month.",
    "ambiguous": "Policy for local times that occur twice during a daylight-saving transition.",
    "nonexistent": "Policy for local times skipped by a daylight-saving transition.",
}


def _humanize_parameter(name: str) -> str:
    return name.strip("_").replace("_", " ") or "argument"


def _operator_parameter_entries(operator: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    """Combine native overload metadata into one user-facing parameter list."""
    entries: dict[str, dict[str, Any]] = {}
    formatter = PublicTypePatternFormatter()
    for overload in _unique_overloads(operator):
        parameters = overload["parameters"]
        for index, parameter in enumerate(parameters):
            name = parameter["name"] or f"arg{index}"
            entry = entries.setdefault(name, {
                "name": name,
                "patterns": [],
                "categories": [],
                "default": False,
                "prefix": "",
            })
            category = parameter["kind"]
            pattern = formatter.format(
                parameter["type_pattern"],
                category="time_series" if category == "time-series" else "scalar",
            )
            if pattern not in entry["patterns"]:
                entry["patterns"].append(pattern)
            if category not in entry["categories"]:
                entry["categories"].append(category)
            entry["default"] |= parameter["has_default"]
            if overload["variadic"] and index == len(parameters) - 1:
                entry["prefix"] = "*"
        if overload["has_kwargs"]:
            entry = entries.setdefault("kwargs", {
                "name": "kwargs",
                "patterns": [],
                "categories": ["time-series"],
                "default": False,
                "prefix": "**",
            })
            pattern = formatter.format(
                overload["kwargs_pattern"] or "time-series", category="time_series")
            if pattern not in entry["patterns"]:
                entry["patterns"].append(pattern)

    for parameter in operator.get("python_parameters", ()):
        name = parameter["name"]
        if name in entries:
            continue
        kind = parameter["kind"]
        entries[name] = {
            "name": name,
            "patterns": ["object"],
            "categories": ["Python argument"],
            "default": parameter["has_default"],
            "prefix": "*" if kind == "VAR_POSITIONAL" else "**" if kind == "VAR_KEYWORD" else "",
        }
    return tuple(entries.values())


def _operator_return_patterns(operator: dict[str, Any]) -> tuple[str, ...]:
    patterns = []
    formatter = PublicTypePatternFormatter()
    groups = (operator, *operator["grouped_overrides"])
    for group in groups:
        for overload in _unique_overloads(group):
            pattern = (
                formatter.format(
                    overload["output_pattern"], category="time_series", output=True)
                if overload["has_output"] else "None"
            )
            if pattern not in patterns:
                patterns.append(pattern)
    return tuple(patterns)


def _example_overload(operator: dict[str, Any]) -> dict[str, Any] | None:
    overloads = _unique_overloads(operator)
    if not overloads:
        for group in operator["grouped_overrides"]:
            overloads.extend(_unique_overloads(group))
    if not overloads:
        return None

    def rank(overload: dict[str, Any]) -> tuple[int, int, int]:
        required = sum(
            not parameter["has_default"] for parameter in overload["parameters"])
        return required, len(overload["parameters"]), bool(overload["variadic"])

    return min(overloads, key=rank)


def _generated_python_example(operator: dict[str, Any]) -> str:
    name = operator["name"]
    python_parameters = operator.get("python_parameters", ())
    use_python_signature = operator["explicit_root"] and any(
        parameter["kind"] not in {"VAR_POSITIONAL", "VAR_KEYWORD"}
        for parameter in python_parameters
    )
    arguments = []
    if use_python_signature:
        for parameter in python_parameters:
            kind = parameter["kind"]
            if parameter["has_default"]:
                continue
            if kind == "VAR_POSITIONAL":
                arguments.append("ts")
            elif kind == "VAR_KEYWORD":
                continue
            elif kind == "KEYWORD_ONLY":
                arguments.append(f"{parameter['name']}={parameter['name']}")
            else:
                arguments.append(parameter["name"])
    else:
        overload = _example_overload(operator)
        if overload is not None:
            parameters = overload["parameters"]
            positional_count = min(overload["positional_params"], len(parameters))
            for index, parameter in enumerate(parameters):
                if parameter["has_default"]:
                    continue
                if overload["variadic"] and index == len(parameters) - 1:
                    arguments.extend(("first", "second"))
                elif index < positional_count:
                    arguments.append(parameter["name"] or f"arg{index}")
                else:
                    parameter_name = parameter["name"] or f"arg{index}"
                    arguments.append(f"{parameter_name}={parameter_name}")
    call = f"hg.{name}({', '.join(arguments)})"
    return_patterns = _operator_return_patterns(operator)
    return call if return_patterns == ("None",) else f"result = {call}"


def _operator_semantic_documentation(
        operator: dict[str, Any], *, include_description: bool = True) -> str:
    """Render semantic sections shared by Sphinx, runtime help and the stub."""
    lines = []
    if include_description:
        lines.extend([
            operator["documentation"]
            or f"Wire ``{operator['name']}`` through native overload resolution.",
            "",
        ])

    parameter_entries = _operator_parameter_entries(operator)
    if parameter_entries:
        lines.extend([
            "Parameters",
            "~~~~~~~~~~",
            "",
            "Time-series inputs are live graph edges. Wiring-time scalar choices",
            "are fixed when the graph is built.",
            "",
        ])
        documented = operator.get("parameter_documentation", {})
        for entry in parameter_entries:
            name = entry["name"]
            display_name = f"{entry['prefix']}{name}"
            categories = ", ".join(entry["categories"])
            patterns = ", ".join(f"``{pattern}``" for pattern in entry["patterns"])
            description = documented.get(name) or _COMMON_PARAMETER_DOCUMENTATION.get(name)
            if description is None:
                description = (
                    f"The {_humanize_parameter(name)} value used by the selected overload."
                )
            if entry["default"]:
                description += " Optional in overloads that show ``= ...``."
            lines.extend([
                f"``{display_name}`` : {categories}; {patterns}",
                f"   {description}",
                "",
            ])

    return_patterns = _operator_return_patterns(operator)
    lines.extend(["Returns", "~~~~~~~", ""])
    return_documentation = operator.get("return_documentation")
    if return_documentation:
        lines.append(return_documentation)
    elif return_patterns == ("None",):
        lines.append("No output. This operator is a sink.")
    elif return_patterns:
        rendered = ", ".join(f"``{pattern}``" for pattern in return_patterns)
        lines.append(
            f"A wired output with one of the overload-selected shapes: {rendered}."
        )
    else:
        lines.append("The output selected by Python helper and native overload resolution.")
    lines.extend(["", "Python example", "~~~~~~~~~~~~~~", "", ".. code-block:: python", ""])
    examples = operator.get("examples") or (_generated_python_example(operator),)
    for example_index, example in enumerate(examples):
        if example_index:
            lines.extend(["", ".. code-block:: python", ""])
        lines.extend(f"   {line}" for line in example.splitlines())
    return "\n".join(lines).rstrip()


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


def _stub_docstring_line(line: str) -> str:
    """Escape source-sensitive text without changing the parsed stub docstring."""
    return line.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')


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
        documentation = _operator_semantic_documentation(operator).splitlines()
        lines.extend([
            f"class {class_name}(_Protocol):",
            '    """' + _stub_docstring_line(documentation[0]),
        ])
        lines.extend(
            f"    {_stub_docstring_line(line)}" if line else ""
            for line in documentation[1:]
        )
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
            documentation = _operator_semantic_documentation(operator)
            lines.append(f"    {operator['name']!r}: {documentation!r},")
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
