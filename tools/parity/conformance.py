"""Run an exact upstream hgraph test tree against both distributions.

The released distribution remains the oracle.  Candidate-only failures are
observations until a narrow manifest rule classifies the exact outcome as an
approved change, a converted internal contract, or a confirmed gap.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import re
import shutil
import subprocess
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .environments import PARITY_ROOT, REPO_ROOT

UPSTREAM_REPOSITORY = "https://github.com/hhenson/hgraph.git"
CONFORMANCE_DEPENDENCIES = (
    "pytest>=7.4.3",
    "black>=25.1.0",
    "duckdb",
    "frozendict",
    "multimethod",
    "ordered-set>=4.1.0",
    "polars>=1.32",
    "psutil",
    "pyarrow>=16.1,<25",
    "pycurl",
    "pytz",
    "sortedcontainers>=2.4.0",
    "sqlalchemy>=2.0",
    "typing-extensions",
)
_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.+-]+)?$")
_ACCEPTED_CLASSIFICATIONS = {"expected-change", "converted"}
_CLASSIFICATIONS = _ACCEPTED_CLASSIFICATIONS | {"confirmed-gap"}
_PASS_LIKE_OUTCOMES = {"passed", "xpassed"}


@dataclass(frozen=True)
class UpstreamSource:
    path: Path
    repository: str
    ref: str
    revision: str
    version: str
    declared_version: str
    test_digest: str


def _run(
    command: list[str],
    *,
    cwd: Path | None = None,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        cwd=cwd,
        capture_output=capture,
        text=True,
    )
    if completed.returncode:
        diagnostic = (completed.stderr or completed.stdout or "")[-4000:]
        raise RuntimeError(
            f"upstream conformance command failed ({completed.returncode}): "
            f"{' '.join(command)}\n{diagnostic}"
        )
    return completed


def _git(path: Path, *arguments: str) -> str:
    return _run(["git", "-C", str(path), *arguments]).stdout.strip()


def _tree_digest(paths: list[Path], *, root: Path) -> str:
    digest = hashlib.sha256()
    for base in paths:
        if not base.exists():
            continue
        for path in sorted(
            item
            for item in base.rglob("*")
            if item.is_file()
            and "__pycache__" not in item.parts
            and item.suffix not in {".pyc", ".pyo"}
        ):
            relative = path.relative_to(root).as_posix()
            digest.update(relative.encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
    return digest.hexdigest()


def _source_version(path: Path) -> str:
    try:
        project = tomllib.loads((path / "pyproject.toml").read_text())["project"]
        return str(project["version"])
    except (OSError, KeyError, tomllib.TOMLDecodeError) as error:
        raise ValueError(
            f"cannot read upstream version from {path}: {error}"
        ) from error


def ensure_upstream_source(
    reference_identity: dict[str, Any],
    *,
    source_path: Path | None = None,
    repository: str = UPSTREAM_REPOSITORY,
) -> UpstreamSource:
    version = str(reference_identity.get("version", ""))
    if not _VERSION.fullmatch(version):
        raise ValueError(f"unsafe or unknown reference version: {version!r}")
    ref = f"v_{version}"
    if source_path is None:
        source_path = PARITY_ROOT / "upstream" / f"hgraph-{ref}"
        if not source_path.exists():
            source_path.parent.mkdir(parents=True, exist_ok=True)
            _run(
                [
                    "git",
                    "clone",
                    "--quiet",
                    "--depth",
                    "1",
                    "--branch",
                    ref,
                    repository,
                    str(source_path),
                ]
            )
    source_path = source_path.absolute()
    if not (source_path / ".git").exists():
        raise ValueError(f"upstream source is not a git checkout: {source_path}")
    if _git(source_path, "status", "--porcelain"):
        raise ValueError(f"upstream source must be unmodified: {source_path}")
    revision = _git(source_path, "rev-parse", "HEAD")
    tags = _git(source_path, "tag", "--points-at", "HEAD").splitlines()
    if ref not in tags:
        raise ValueError(f"upstream source revision {revision} is not tagged {ref}")
    # hgraph's release automation tags the release source before its follow-up
    # pyproject version bump.  The exact release tag is authoritative; retain
    # the in-tree declaration as provenance instead of silently selecting a
    # later commit or rejecting the tagged source.
    declared_version = _source_version(source_path)
    test_roots = [source_path / "hgraph_unit_tests", source_path / "examples"]
    if not test_roots[0].is_dir():
        raise ValueError(f"upstream tests are missing from {source_path}")
    return UpstreamSource(
        path=source_path,
        repository=repository,
        ref=ref,
        revision=revision,
        version=version,
        declared_version=declared_version,
        test_digest=_tree_digest(test_roots, root=source_path),
    )


def prepare_test_workspace(source: UpstreamSource) -> Path:
    workspace = PARITY_ROOT / "upstream-workspaces" / source.revision
    marker = workspace / ".test-digest"
    current = marker.read_text().strip() if marker.exists() else ""
    if current == source.test_digest:
        copied_digest = _tree_digest(
            [workspace / "hgraph_unit_tests", workspace / "examples"],
            root=workspace,
        )
        if copied_digest == source.test_digest:
            return workspace
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True)
    for name in ("hgraph_unit_tests", "examples"):
        source_path = source.path / name
        if source_path.exists():
            shutil.copytree(source_path, workspace / name)
    (workspace / "pytest.ini").write_text(
        "[pytest]\n"
        "addopts = --import-mode=importlib\n"
        "markers =\n"
        "    serial: upstream serial test\n"
        "    smoke: upstream smoke test\n"
    )
    copied_digest = _tree_digest(
        [workspace / "hgraph_unit_tests", workspace / "examples"],
        root=workspace,
    )
    if copied_digest != source.test_digest:
        raise RuntimeError("staged upstream tests differ from the tagged source")
    marker.write_text(source.test_digest + "\n")
    return workspace


def install_conformance_dependencies(
    interpreters: tuple[Path, ...],
    *,
    extras: tuple[str, ...] = (),
) -> None:
    if not interpreters:
        return
    reference = interpreters[0]
    _run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(reference),
            *CONFORMANCE_DEPENDENCIES,
            *extras,
        ]
    )
    resolved = conformance_environment(reference, extras=extras)
    exact = [
        f"{name}=={version}" for name, version in sorted(resolved["packages"].items())
    ]
    for interpreter in interpreters[1:]:
        _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(interpreter),
                *exact,
            ]
        )


def _distribution_name(requirement: str) -> str:
    return re.split(r"[<>=!~\[]", requirement, maxsplit=1)[0].strip()


def conformance_environment(
    interpreter: Path,
    *,
    extras: tuple[str, ...] = (),
) -> dict[str, Any]:
    distributions = sorted(
        {
            _distribution_name(requirement)
            for requirement in (*CONFORMANCE_DEPENDENCIES, *extras)
        }
    )
    program = (
        "import importlib.metadata as m,json,platform,sys;"
        f"names={distributions!r};"
        "print(json.dumps({'python':platform.python_version(),"
        "'implementation':platform.python_implementation(),"
        "'packages':{name:m.version(name) for name in names}},sort_keys=True))"
    )
    completed = _run([str(interpreter), "-c", program])
    return json.loads(completed.stdout)


def require_aligned_conformance_environments(
    reference_python: Path,
    candidate_python: Path,
    *,
    extras: tuple[str, ...] = (),
) -> dict[str, Any]:
    reference = conformance_environment(reference_python, extras=extras)
    candidate = conformance_environment(candidate_python, extras=extras)
    if reference != candidate:
        raise RuntimeError(
            "reference and candidate conformance dependencies are not aligned: "
            f"reference={reference}, candidate={candidate}"
        )
    return reference


def _sanitized_environment() -> dict[str, str]:
    environment = os.environ.copy()
    for name in ("HGRAPH_USE_CPP", "PYTHONHOME", "VIRTUAL_ENV"):
        environment.pop(name, None)
    environment.update(
        PYTHONHASHSEED="0",
        PYTHONNOUSERSITE="1",
        PYTHONPATH=str(REPO_ROOT),
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
        TZ="UTC",
    )
    return environment


def run_upstream_suite(
    interpreter: Path,
    workspace: Path,
    selectors: list[str],
    *,
    result_path: Path,
    timeout_seconds: float,
    excluded_paths: list[str] | None = None,
) -> dict[str, Any]:
    result_path = result_path.absolute()
    result_path.unlink(missing_ok=True)
    command = [
        str(interpreter),
        "-m",
        "pytest",
        "-p",
        "tools.parity.pytest_plugin",
        "--hgraph-conformance-report",
        str(result_path),
        "-c",
        str(workspace / "pytest.ini"),
        "--rootdir",
        str(workspace),
        "--continue-on-collection-errors",
        "-q",
    ]
    for excluded_path in excluded_paths or ():
        command.extend(("--ignore", excluded_path))
    command.extend(selectors)
    try:
        completed = subprocess.run(
            command,
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=_sanitized_environment(),
        )
    except subprocess.TimeoutExpired as error:
        return {
            "schema_version": 1,
            "status": "timeout",
            "timeout_seconds": timeout_seconds,
            "stdout": error.stdout or "",
            "stderr": error.stderr or "",
            "tests": {},
            "collected": [],
            "collection_errors": [],
        }
    try:
        result = json.loads(result_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        return {
            "schema_version": 1,
            "status": "infrastructure-error",
            "diagnostic": f"pytest did not produce a result: {error}",
            "process_returncode": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "tests": {},
            "collected": [],
            "collection_errors": [],
        }
    result.update(
        status="complete",
        process_returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )
    return result


def _safe_relative(value: str, *, field: str) -> str:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{field} must be a repository-relative path: {value}")
    return path.as_posix()


def load_conformance_manifest(path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read conformance manifest {path}: {error}") from error
    if manifest.get("schema_version") != 1:
        raise ValueError("conformance manifest schema_version must be 1")
    profiles = manifest.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        raise ValueError("conformance manifest requires profiles")
    for profile, selectors in profiles.items():
        if (
            not isinstance(profile, str)
            or not isinstance(selectors, list)
            or not selectors
        ):
            raise ValueError("conformance profiles require a name and selectors")
        for selector in selectors:
            value = _safe_relative(selector, field=f"profile {profile}")
            if not value.startswith("hgraph_unit_tests"):
                raise ValueError(f"profile selector is outside upstream tests: {value}")
    exclusions = manifest.get("exclusions", [])
    if not isinstance(exclusions, list):
        raise ValueError("conformance manifest exclusions must be a list")
    excluded_paths: set[str] = set()
    for exclusion in exclusions:
        if not isinstance(exclusion, dict):
            raise ValueError("conformance exclusions must be objects")
        raw_path = exclusion.get("path")
        if not isinstance(raw_path, str) or not raw_path:
            raise ValueError("conformance exclusion requires a path")
        excluded_path = _safe_relative(raw_path, field="conformance exclusion")
        if (
            not excluded_path.startswith("hgraph_unit_tests/")
            or not excluded_path.endswith(".py")
            or "::" in excluded_path
            or any(character in excluded_path for character in "*?[]")
        ):
            raise ValueError(
                "conformance exclusion must name one exact upstream test module: "
                f"{excluded_path}"
            )
        if excluded_path in excluded_paths:
            raise ValueError(f"duplicate conformance exclusion: {excluded_path}")
        excluded_paths.add(excluded_path)
        if not isinstance(exclusion.get("reason"), str) or not exclusion["reason"]:
            raise ValueError(
                f"conformance exclusion {excluded_path} requires a reason"
            )
        try:
            date.fromisoformat(exclusion["review_date"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                f"conformance exclusion {excluded_path} requires an ISO review_date"
            ) from error
    rules = manifest.get("rules", [])
    if not isinstance(rules, list):
        raise ValueError("conformance manifest rules must be a list")
    ids: set[str] = set()
    for rule in rules:
        rule_id = rule.get("id")
        if not isinstance(rule_id, str) or not rule_id or rule_id in ids:
            raise ValueError(f"invalid or duplicate conformance rule id: {rule_id!r}")
        ids.add(rule_id)
        if rule.get("classification") not in _CLASSIFICATIONS:
            raise ValueError(f"invalid classification for rule {rule_id}")
        if not isinstance(rule.get("match"), str) or not rule["match"]:
            raise ValueError(f"rule {rule_id} requires a node-id match")
        if not isinstance(rule.get("reason"), str) or not rule["reason"]:
            raise ValueError(f"rule {rule_id} requires a reason")
        try:
            date.fromisoformat(rule["review_date"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"rule {rule_id} requires an ISO review_date") from error
        outcomes = rule.get("candidate_outcomes")
        if not isinstance(outcomes, list) or not outcomes:
            raise ValueError(f"rule {rule_id} requires candidate_outcomes")
        reference_outcomes = rule.get(
            "reference_outcomes", sorted(_PASS_LIKE_OUTCOMES)
        )
        if not isinstance(reference_outcomes, list) or not reference_outcomes:
            raise ValueError(f"rule {rule_id} requires reference_outcomes")
        non_passing_reference = set(reference_outcomes) - _PASS_LIKE_OUTCOMES
        if non_passing_reference:
            if (
                rule["classification"] != "converted"
                or non_passing_reference != {"skipped"}
            ):
                raise ValueError(
                    f"rule {rule_id} may only convert a skipped reference outcome"
                )
            reference_pattern = rule.get("reference_diagnostic_regex")
            if not reference_pattern:
                raise ValueError(
                    f"skipped-reference rule {rule_id} requires "
                    "reference_diagnostic_regex"
                )
            try:
                re.compile(reference_pattern)
            except re.error as error:
                raise ValueError(
                    f"invalid reference_diagnostic_regex for {rule_id}: {error}"
                ) from error
        isolation_outcomes = rule.get("isolate_reference_outcomes")
        if isolation_outcomes is not None:
            if (
                rule["classification"] != "converted"
                or isolation_outcomes != ["xfailed"]
            ):
                raise ValueError(
                    f"rule {rule_id} may only isolate an xfailed converted test"
                )
        if rule["classification"] in _ACCEPTED_CLASSIFICATIONS:
            if not rule.get("diagnostic_regex"):
                raise ValueError(f"accepted rule {rule_id} requires diagnostic_regex")
            try:
                re.compile(rule["diagnostic_regex"])
            except re.error as error:
                raise ValueError(
                    f"invalid diagnostic_regex for {rule_id}: {error}"
                ) from error
            if not rule.get("decision"):
                raise ValueError(f"accepted rule {rule_id} requires a decision")
        if rule["classification"] == "converted":
            evidence = rule.get("evidence")
            if not isinstance(evidence, list) or not evidence:
                raise ValueError(f"converted rule {rule_id} requires evidence")
            for evidence_path in evidence:
                relative = _safe_relative(
                    evidence_path, field=f"rule {rule_id} evidence"
                )
                if not (REPO_ROOT / relative).exists():
                    raise ValueError(
                        f"converted rule {rule_id} evidence does not exist: {relative}"
                    )
    return manifest


def profile_selectors(manifest: dict[str, Any], profile: str) -> list[str]:
    try:
        return list(manifest["profiles"][profile])
    except KeyError as error:
        choices = ", ".join(sorted(manifest["profiles"]))
        raise ValueError(
            f"unknown conformance profile {profile!r}; choose {choices}"
        ) from error


def conformance_exclusions(manifest: dict[str, Any]) -> list[str]:
    return [exclusion["path"] for exclusion in manifest.get("exclusions", [])]


def validate_selectors(selectors: list[str]) -> list[str]:
    validated: list[str] = []
    for selector in selectors:
        path = selector.split("::", 1)[0]
        value = _safe_relative(path, field="conformance selector")
        if not value.startswith("hgraph_unit_tests"):
            raise ValueError(f"conformance selector is outside upstream tests: {value}")
        validated.append(selector.replace("\\", "/"))
    if not validated:
        raise ValueError("at least one upstream test selector is required")
    return validated


def reference_isolation_selectors(
    reference: dict[str, Any], manifest: dict[str, Any]
) -> list[str]:
    """Return suite-context XFAILs explicitly approved for isolated replay."""

    selectors: list[str] = []
    for raw_nodeid, result in reference.get("tests", {}).items():
        nodeid = _normalize_nodeid(raw_nodeid)
        outcome = result.get("outcome")
        if any(
            outcome in rule.get("isolate_reference_outcomes", ())
            and fnmatch.fnmatchcase(nodeid, rule["match"])
            for rule in manifest.get("rules", ())
        ):
            selectors.append(nodeid)
    return sorted(selectors)


def apply_reference_isolation(
    reference: dict[str, Any], nodeid: str, isolated: dict[str, Any]
) -> dict[str, Any]:
    """Use a pass-like isolated result while retaining the suite result."""

    normalized = _normalize_nodeid(nodeid)
    reference_key = next(
        (
            key
            for key in reference.get("tests", {})
            if _normalize_nodeid(key) == normalized
        ),
        None,
    )
    isolated_key = next(
        (
            key
            for key in isolated.get("tests", {})
            if _normalize_nodeid(key) == normalized
        ),
        None,
    )
    suite_result = (
        reference.get("tests", {}).get(reference_key) if reference_key else None
    )
    isolated_result = (
        isolated.get("tests", {}).get(isolated_key) if isolated_key else None
    )
    applied = bool(
        reference_key
        and suite_result
        and suite_result.get("outcome") == "xfailed"
        and isolated_result
        and isolated_result.get("outcome") in _PASS_LIKE_OUTCOMES
    )
    if applied:
        reference["tests"][reference_key] = {
            **isolated_result,
            "isolated_reference": True,
            "suite_result": suite_result,
        }
    return {
        "nodeid": normalized,
        "applied": applied,
        "suite_result": suite_result,
        "isolated_result": isolated_result,
    }


def _normalize_nodeid(nodeid: str) -> str:
    return nodeid.replace("\\", "/")


def _collection_error_for(
    nodeid: str, errors: list[dict[str, Any]]
) -> dict[str, Any] | None:
    file_path = nodeid.split("::", 1)[0]
    for error in errors:
        error_node = _normalize_nodeid(str(error.get("nodeid", "")))
        if error_node == file_path or file_path.startswith(
            error_node.rstrip("/") + "/"
        ):
            return {
                "outcome": "collection-error",
                "phase": "collection",
                "diagnostic": error.get("diagnostic", ""),
                "duration": 0.0,
            }
    return None


def _rule_matches(
    rule: dict[str, Any],
    *,
    nodeid: str,
    reference: dict[str, Any],
    candidate: dict[str, Any],
) -> bool:
    if not fnmatch.fnmatchcase(nodeid, rule["match"]):
        return False
    if candidate.get("outcome") not in rule["candidate_outcomes"]:
        return False
    reference_outcomes = rule.get(
        "reference_outcomes", sorted(_PASS_LIKE_OUTCOMES)
    )
    if reference.get("outcome") not in reference_outcomes:
        return False
    pattern = rule.get("diagnostic_regex")
    if pattern and re.search(
        pattern, candidate.get("diagnostic", ""), re.DOTALL
    ) is None:
        return False
    reference_pattern = rule.get("reference_diagnostic_regex")
    return not reference_pattern or re.search(
        reference_pattern, reference.get("diagnostic", ""), re.DOTALL
    ) is not None


def compare_upstream_results(
    reference: dict[str, Any],
    candidate: dict[str, Any],
    manifest: dict[str, Any],
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": 1,
        "matched": [],
        "known_expected": [],
        "review_required": [],
        "confirmed_gaps": [],
        "reference_unverified": [],
        "candidate_only": [],
        "ambiguous_rules": [],
    }
    reference_tests = {
        _normalize_nodeid(nodeid): value
        for nodeid, value in reference.get("tests", {}).items()
    }
    candidate_tests = {
        _normalize_nodeid(nodeid): value
        for nodeid, value in candidate.get("tests", {}).items()
    }
    candidate_errors = candidate.get("collection_errors", [])
    for nodeid in sorted(set(reference_tests) | set(candidate_tests)):
        ref = reference_tests.get(nodeid)
        cand = candidate_tests.get(nodeid)
        if ref is None:
            report["candidate_only"].append({"nodeid": nodeid, "candidate": cand})
            continue
        if cand is None:
            cand = _collection_error_for(nodeid, candidate_errors) or {
                "outcome": "not-run",
                "phase": "unknown",
                "diagnostic": "candidate did not report this reference test",
                "duration": 0.0,
            }
        entry = {"nodeid": nodeid, "reference": ref, "candidate": cand}
        reference_outcome = ref.get("outcome")
        if (
            reference_outcome in _PASS_LIKE_OUTCOMES
            and cand.get("outcome") in _PASS_LIKE_OUTCOMES
        ):
            report["matched"].append(entry)
            continue
        matching_rules = [
            rule
            for rule in manifest.get("rules", [])
            if _rule_matches(
                rule,
                nodeid=nodeid,
                reference=ref,
                candidate=cand,
            )
        ]
        if len(matching_rules) > 1:
            entry["rules"] = [rule["id"] for rule in matching_rules]
            report["ambiguous_rules"].append(entry)
            continue
        if reference_outcome not in _PASS_LIKE_OUTCOMES and not matching_rules:
            report["reference_unverified"].append(entry)
            continue
        if not matching_rules:
            report["review_required"].append(entry)
            continue
        rule = matching_rules[0]
        entry["rule"] = rule
        if rule["classification"] in _ACCEPTED_CLASSIFICATIONS:
            report["known_expected"].append(entry)
        else:
            report["confirmed_gaps"].append(entry)
    for error in reference.get("collection_errors", []):
        report["reference_unverified"].append(
            {
                "nodeid": _normalize_nodeid(str(error.get("nodeid", "collection"))),
                "reference": error,
                "candidate": {"outcome": "not-compared", "diagnostic": ""},
            }
        )
    report["summary"] = {
        key: len(report[key])
        for key in (
            "matched",
            "known_expected",
            "review_required",
            "confirmed_gaps",
            "reference_unverified",
            "candidate_only",
            "ambiguous_rules",
        )
    }
    report["summary"].update(
        reference_collected=len(reference.get("collected", [])),
        candidate_collected=len(candidate.get("collected", [])),
        reference_collection_errors=len(reference.get("collection_errors", [])),
        candidate_collection_errors=len(candidate.get("collection_errors", [])),
    )
    return report


def _brief_diagnostic(entry: dict[str, Any]) -> str:
    diagnostic = entry["candidate"].get("diagnostic", "").strip()
    if not diagnostic:
        diagnostic = entry["reference"].get("diagnostic", "").strip()
    if not diagnostic:
        return entry["candidate"].get("outcome", "unknown")
    lines = [line.strip() for line in diagnostic.splitlines() if line.strip()]
    return (lines[-1] if lines else diagnostic)[:300]


def render_conformance_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    source = report.get("source", {})
    lines = ["# Upstream hgraph conformance", ""]
    lines.append(
        f"Reference: hgraph {report['reference_identity']['version']}; "
        f"candidate: hgraph {report['candidate_identity']['version']}"
    )
    lines.append(
        f"Upstream source: `{source.get('ref')}` at `{source.get('revision')}` "
        f"(test digest `{source.get('test_digest')}`)"
    )
    environment = report.get("conformance_environment", {})
    if environment:
        lines.append(
            f"Aligned environment: {environment.get('implementation')} "
            f"{environment.get('python')} with {len(environment.get('packages', {}))} "
            "pinned conformance dependencies."
        )
    if source.get("declared_version") != source.get("version"):
        lines.append(
            f"Tagged source declares version `{source.get('declared_version')}`; "
            f"the installed release and tag identify `{source.get('version')}`."
        )
    lines.extend(
        [
            "",
            f"- reference tests collected: {summary['reference_collected']}",
            f"- exact candidate matches: {summary['matched']}",
            f"- known expected/converted outcomes: {summary['known_expected']}",
            "- reference XFAILs verified by isolated replay: "
            f"{summary.get('reference_isolated', 0)}",
            f"- review required (not yet classified as defects): {summary['review_required']}",
            f"- confirmed compatibility gaps: {summary['confirmed_gaps']}",
            f"- reference-unverified tests: {summary['reference_unverified']}",
            f"- ambiguous known rules: {summary['ambiguous_rules']}",
            "",
        ]
    )
    exclusions = report.get("exclusions", [])
    if exclusions:
        lines.extend(["## Excluded experimental modules", ""])
        for exclusion in exclusions:
            lines.append(f"- `{exclusion['path']}`: {exclusion['reason']}")
        lines.append("")
    for heading, key in (
        ("Review required", "review_required"),
        ("Confirmed compatibility gaps", "confirmed_gaps"),
        ("Known expected or converted outcomes", "known_expected"),
        ("Reference-unverified", "reference_unverified"),
        ("Ambiguous rules", "ambiguous_rules"),
    ):
        entries = report.get(key, [])
        if not entries:
            continue
        lines.extend([f"## {heading}", ""])
        limit = 100
        for entry in entries[:limit]:
            detail = _brief_diagnostic(entry)
            rule = entry.get("rule")
            suffix = f" — rule `{rule['id']}`" if rule else ""
            lines.append(f"- `{entry['nodeid']}`: {detail}{suffix}")
        if len(entries) > limit:
            lines.append(
                f"- ... {len(entries) - limit} additional entries are in report.json"
            )
        lines.append("")
    return "\n".join(lines) + "\n"
