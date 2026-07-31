"""API-surface parity audit (issue-discovery campaign, 2026-07).

Behavioral fuzzing cannot see a mis-ported API: a missing function, a
renamed parameter, a changed default, or an adaptor module that fails to
import never produces a differential trace — the wiring just breaks in
client code. This audit compares the PUBLIC SURFACE of ``hgraph`` and
every ``hgraph.adaptors`` submodule between the reference and candidate
environments: exported names, callable signatures (parameters, kinds,
defaults), class methods, and module importability. Asymmetric import
failures are findings, not errors — heavy adaptor dependencies are meant
to be lazy-imported, so a module that imports in the reference must
import in the candidate.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

_PROBE = r"""
import importlib, inspect, json, pkgutil, re, sys

_ADDRESS = re.compile(r" at 0x[0-9a-fA-F]+")

def _default_repr(value):
    # Identity sentinels (``object()`` defaults) repr with a process-local
    # address; normalize so two probes of the same implementation agree.
    return _ADDRESS.sub("", repr(value))

def describe_callable(obj):
    try:
        signature = inspect.signature(obj)
    except (ValueError, TypeError):
        return {"signature": None}
    params = []
    for p in signature.parameters.values():
        params.append({
            "name": p.name,
            "kind": str(p.kind),
            "default": _default_repr(p.default) if p.default is not inspect.Parameter.empty else None,
        })
    return {"signature": params}

def describe_module(name):
    try:
        module = importlib.import_module(name)
    except BaseException as error:  # noqa: BLE001 - import asymmetry IS the finding
        return {"import_error": f"{type(error).__name__}: {error}"}
    exported = getattr(module, "__all__", None)
    names = exported if exported is not None else [
        n for n in dir(module) if not n.startswith("_")
    ]
    surface = {}
    for name_ in sorted(set(names)):
        try:
            obj = getattr(module, name_)
        except AttributeError:
            surface[name_] = {"kind": "missing-attr"}
            continue
        if inspect.isclass(obj):
            # The constructor IS public API: required-parameter changes must
            # surface even when the method map is unchanged.
            try:
                constructor = describe_callable(obj)
            except BaseException:  # noqa: BLE001
                constructor = {"signature": None}
            methods = {}
            for m in sorted(dir(obj)):
                if m.startswith("_"):
                    continue
                attr = inspect.getattr_static(obj, m, None)
                if callable(attr) or isinstance(attr, (staticmethod, classmethod)):
                    try:
                        methods[m] = describe_callable(getattr(obj, m))
                    except BaseException:  # noqa: BLE001
                        methods[m] = {"signature": None}
            surface[name_] = {"kind": "class", "methods": methods,
                              "constructor": constructor.get("signature")}
        elif callable(obj):
            entry = describe_callable(obj)
            entry["kind"] = "callable"
            surface[name_] = entry
        else:
            surface[name_] = {"kind": type(obj).__name__}
    return {"surface": surface, "has_all": exported is not None}

modules = ["hgraph", "hgraph.test", "hgraph.adaptors"]

def _collect(package_name):
    # Recursive discovery: nested paths (hgraph.adaptors.sql.sql_connection)
    # break independently of their parent package. Each discovered module is
    # probed via describe_module, which contains its own import guard.
    try:
        package = importlib.import_module(package_name)
    except BaseException:  # noqa: BLE001
        return
    for info in pkgutil.iter_modules(getattr(package, "__path__", [])):
        qualified = f"{package_name}.{info.name}"
        modules.append(qualified)
        if info.ispkg:
            _collect(qualified)

_collect("hgraph.adaptors")

print(json.dumps({name: describe_module(name) for name in sorted(set(modules))}))
"""


def probe_surface(python: Path | str) -> dict[str, Any]:
    result = subprocess.run(
        [str(python), "-c", _PROBE],
        capture_output=True,
        text=True,
        timeout=300,
        check=True,
    )
    return json.loads(result.stdout)


def _signature_key(entry: dict[str, Any]) -> Any:
    return entry.get("signature")


def compare_surfaces(
    reference: dict[str, Any], candidate: dict[str, Any]
) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    for module in sorted(set(reference) | set(candidate)):
        ref, cand = reference.get(module), candidate.get(module)
        if ref is None or cand is None:
            findings.append({
                "module": module,
                "kind": "module-missing",
                "side": "candidate" if cand is None else "reference",
            })
            continue
        ref_err, cand_err = ref.get("import_error"), cand.get("import_error")
        if ref_err or cand_err:
            if bool(ref_err) != bool(cand_err):
                findings.append({
                    "module": module,
                    "kind": "import-asymmetry",
                    "reference_error": ref_err,
                    "candidate_error": cand_err,
                })
            continue
        ref_surface, cand_surface = ref["surface"], cand["surface"]
        for name in sorted(set(ref_surface) | set(cand_surface)):
            r, c = ref_surface.get(name), cand_surface.get(name)
            if r is None or c is None:
                findings.append({
                    "module": module,
                    "name": name,
                    "kind": "name-missing",
                    "side": "candidate" if c is None else "reference",
                })
                continue
            if r.get("kind") != c.get("kind"):
                findings.append({
                    "module": module,
                    "name": name,
                    "kind": "kind-mismatch",
                    "reference": r.get("kind"),
                    "candidate": c.get("kind"),
                })
                continue
            if r.get("kind") == "callable" and _signature_key(r) != _signature_key(c):
                findings.append({
                    "module": module,
                    "name": name,
                    "kind": "signature-mismatch",
                    "reference": _signature_key(r),
                    "candidate": _signature_key(c),
                })
            if r.get("kind") == "class":
                if r.get("constructor") != c.get("constructor"):
                    findings.append({
                        "module": module,
                        "name": f"{name}.__init__",
                        "kind": "constructor-signature-mismatch",
                        "reference": r.get("constructor"),
                        "candidate": c.get("constructor"),
                    })
                rm, cm = r.get("methods", {}), c.get("methods", {})
                for method in sorted(set(rm) | set(cm)):
                    mr, mc = rm.get(method), cm.get(method)
                    if mr is None or mc is None:
                        findings.append({
                            "module": module,
                            "name": f"{name}.{method}",
                            "kind": "method-missing",
                            "side": "candidate" if mc is None else "reference",
                        })
                    elif _signature_key(mr) != _signature_key(mc):
                        findings.append({
                            "module": module,
                            "name": f"{name}.{method}",
                            "kind": "method-signature-mismatch",
                            "reference": _signature_key(mr),
                            "candidate": _signature_key(mc),
                        })
    return {
        "schema_version": 1,
        "modules_compared": sorted(set(reference) | set(candidate)),
        "findings": findings,
    }


def render_surface_markdown(report: dict[str, Any]) -> str:
    lines = ["# API surface parity", ""]
    findings = report["findings"]
    lines.append(f"Modules compared: {len(report['modules_compared'])}; findings: {len(findings)}")
    lines.append("")
    by_module: dict[str, list[dict[str, Any]]] = {}
    for finding in findings:
        by_module.setdefault(finding["module"], []).append(finding)
    for module, entries in sorted(by_module.items()):
        lines.append(f"## {module}")
        for entry in entries:
            detail = {k: v for k, v in entry.items() if k != "module"}
            lines.append(f"- `{json.dumps(detail, sort_keys=True)}`")
        lines.append("")
    return "\n".join(lines) + "\n"
