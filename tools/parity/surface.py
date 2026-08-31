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

import fnmatch
import importlib
import inspect
import json
import pkgutil
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

#: One shared dependency list for ``--with-extras``: installed into BOTH
#: environments so adaptor imports are symmetric by construction (the two
#: packages spell their extras differently — upstream ``web``/``messaging``
#: vs the candidate's per-adaptor extras — so extras names cannot be used).
SURFACE_EXTRA_DEPENDENCIES: tuple[str, ...] = (
    "tornado>=6.5",
    "perspective-python<5.0.0",
    "requests",
    "pandas>=2.0",
    "pydantic>=2,<3",
    "kafka-python>=2.1.5",
    "boto3>=1.34",
    "deltalake>=1.0",
    "polars>=1.32",
    "sqlalchemy>=2.0",
    "duckdb>=1.4",
    "connectorx>=0.4.5",
    "adbc-driver-snowflake>=1.8",
)

_ADDRESS = re.compile(r" at 0x[0-9a-fA-F]+")


def _default_repr(value):
    # Identity sentinels (``object()`` defaults) repr with a process-local
    # address; normalize so two probes of the same implementation agree.
    return _ADDRESS.sub("", repr(value))


def _describe_callable(obj):
    try:
        signature = inspect.signature(obj)
    except (ValueError, TypeError):
        return {"signature": None}
    params = []
    for p in signature.parameters.values():
        params.append({
            "name": p.name,
            "kind": str(p.kind),
            "default": (
                _default_repr(p.default)
                if p.default is not inspect.Parameter.empty
                else None
            ),
        })
    return {"signature": params}


def _error_description(error):
    return f"{type(error).__name__}: {error}"


def _safe_describe_callable(obj):
    try:
        return _describe_callable(obj)
    except Exception:  # A broken signature is surface data, not a probe failure.
        return {"signature": None}


def _describe_class(obj):
    methods = {}
    for name in sorted(member for member in dir(obj) if not member.startswith("_")):
        attr = inspect.getattr_static(obj, name, None)
        if callable(attr) or isinstance(attr, (staticmethod, classmethod)):
            try:
                method = getattr(obj, name)
            except Exception:  # A descriptor may fail while being resolved.
                methods[name] = {"signature": None}
            else:
                methods[name] = _safe_describe_callable(method)
    return {
        "kind": "class",
        "methods": methods,
        "constructor": _safe_describe_callable(obj)["signature"],
    }


def _describe_export(module, name):
    try:
        obj = getattr(module, name)
    except Exception as error:  # Lazy optional imports are recorded per export.
        return {
            "kind": "attribute-error",
            "error": _error_description(error),
        }
    if inspect.isclass(obj):
        return _describe_class(obj)
    if callable(obj):
        return {"kind": "callable", **_describe_callable(obj)}
    return {"kind": type(obj).__name__}


def _describe_module(name):
    try:
        module = importlib.import_module(name)
    except Exception as error:  # Import asymmetry is a finding.
        return {"import_error": _error_description(error)}
    exported = getattr(module, "__all__", None)
    names = (
        exported
        if exported is not None
        else [member for member in dir(module) if not member.startswith("_")]
    )
    surface = {
        name: _describe_export(module, name)
        for name in sorted(set(names))
    }
    return {"surface": surface, "has_all": exported is not None}


def _collect_modules(package_name, modules):
    # Recursive discovery: nested paths (hgraph.adaptors.sql.sql_connection)
    # break independently of their parent package. Each discovered module is
    # probed via describe_module, which contains its own import guard.
    try:
        package = importlib.import_module(package_name)
    except Exception:
        return
    for info in pkgutil.iter_modules(getattr(package, "__path__", [])):
        qualified = f"{package_name}.{info.name}"
        modules.append(qualified)
        if info.ispkg:
            _collect_modules(qualified, modules)


def _probe_payload():
    modules = ["hgraph", "hgraph.test", "hgraph.adaptors"]
    _collect_modules("hgraph.adaptors", modules)
    return {
        name: _describe_module(name)
        for name in sorted(set(modules))
    }


def _probe_main():
    print(json.dumps(_probe_payload()))


def probe_surface(python: Path | str) -> dict[str, Any]:
    result = subprocess.run(
        [
            str(python),
            "-c",
            "from tools.parity.surface import _probe_main; _probe_main()",
        ],
        capture_output=True,
        text=True,
        timeout=300,
        cwd=Path(__file__).resolve().parents[2],
    )
    if result.returncode:
        raise RuntimeError(
            f"surface probe failed under {python}:\n{result.stderr.strip()}"
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
            if (
                r.get("kind") == "attribute-error"
                and r.get("error") != c.get("error")
            ):
                findings.append({
                    "module": module,
                    "name": name,
                    "kind": "attribute-error-mismatch",
                    "reference_error": r.get("error"),
                    "candidate_error": c.get("error"),
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


def _finding_side(finding: dict[str, Any]) -> str | None:
    if finding["kind"] == "import-asymmetry":
        return "reference" if finding.get("reference_error") else "candidate"
    return finding.get("side")


def load_known_surface(path: Path | str) -> list[dict[str, Any]]:
    data = json.loads(Path(path).read_text())
    rules = data["rules"] if isinstance(data, dict) else data
    for rule in rules:
        if "reason" not in rule:
            raise ValueError(f"surface known-rule missing a reason: {rule}")
    return rules


def classify_findings(
    findings: list[dict[str, Any]], rules: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split findings into (actionable, accepted-with-reason)."""

    def matches(rule: dict[str, Any], finding: dict[str, Any]) -> bool:
        if not fnmatch.fnmatchcase(finding["module"], rule.get("module", "*")):
            return False
        if "name" in rule and not fnmatch.fnmatchcase(
            finding.get("name", ""), rule["name"]
        ):
            return False
        if rule.get("kind", "*") not in ("*", finding["kind"]):
            return False
        if "side" in rule and rule["side"] != _finding_side(finding):
            return False
        for field in (
            "reference",
            "candidate",
            "reference_error",
            "candidate_error",
        ):
            if field in rule and rule[field] != finding.get(field):
                return False
        return True

    actionable: list[dict[str, Any]] = []
    accepted: list[dict[str, Any]] = []
    for finding in findings:
        rule = next((r for r in rules if matches(r, finding)), None)
        if rule is None:
            actionable.append(finding)
        else:
            accepted.append({**finding, "accepted_reason": rule["reason"]})
    return actionable, accepted


def render_surface_markdown(report: dict[str, Any]) -> str:
    lines = ["# API surface parity", ""]
    findings = report.get("actionable", report["findings"])
    accepted = report.get("accepted", [])
    lines.append(
        f"Modules compared: {len(report['modules_compared'])}; "
        f"actionable findings: {len(findings)}; accepted (known): {len(accepted)}"
    )
    lines.append("")
    if accepted:
        reasons: dict[str, int] = {}
        for entry in accepted:
            reasons[entry["accepted_reason"]] = reasons.get(entry["accepted_reason"], 0) + 1
        lines.append("Accepted by reason:")
        for reason, count in sorted(reasons.items()):
            lines.append(f"- {count} — {reason}")
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
