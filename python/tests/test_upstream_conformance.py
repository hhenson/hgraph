"""Controller tests for exact upstream hgraph conformance execution."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from tools.parity.conformance import (
    UpstreamSource,
    apply_reference_isolation,
    compare_upstream_results,
    install_conformance_dependencies,
    load_conformance_manifest,
    prepare_test_workspace,
    profile_selectors,
    reference_isolation_selectors,
    require_aligned_conformance_environments,
    run_upstream_suite,
    validate_selectors,
)


def _result(tests=None, *, collected=None, collection_errors=None):
    return {
        "status": "complete",
        "tests": tests or {},
        "collected": collected or list((tests or {}).keys()),
        "collection_errors": collection_errors or [],
    }


def _outcome(outcome, diagnostic=""):
    return {
        "outcome": outcome,
        "phase": "call",
        "diagnostic": diagnostic,
        "duration": 0.01,
    }


def _manifest(rules=None):
    return {
        "schema_version": 1,
        "profiles": {"core": ["hgraph_unit_tests"]},
        "rules": rules or [],
    }


def test_conformance_classifies_only_narrow_known_outcomes():
    nodeid = "hgraph_unit_tests/_types/test_metadata.py::test_parse"
    reference = _result({nodeid: _outcome("passed")})
    candidate = _result(
        {
            nodeid: _outcome(
                "failed",
                "ImportError: cannot import name 'HgTypeMetaData' from 'hgraph'",
            )
        }
    )
    rule = {
        "id": "metadata-reflection-conversion",
        "match": "hgraph_unit_tests/_types/test_metadata.py::*",
        "classification": "converted",
        "reference_outcomes": ["passed"],
        "candidate_outcomes": ["failed"],
        "diagnostic_regex": "HgTypeMetaData",
        "reason": "the public reflection API replaces private metadata objects",
        "decision": "docs/source/developer_guide/parity_matrix.rst",
        "review_date": "2026-08-05",
        "evidence": ["python/tests/test_reflection.py"],
    }

    report = compare_upstream_results(reference, candidate, _manifest([rule]))
    assert report["summary"]["known_expected"] == 1
    assert report["known_expected"][0]["rule"]["id"] == rule["id"]

    candidate["tests"][nodeid]["diagnostic"] = "AssertionError: wrong value"
    report = compare_upstream_results(reference, candidate, _manifest([rule]))
    assert report["summary"]["known_expected"] == 0
    assert report["summary"]["review_required"] == 1


def test_conformance_expands_candidate_collection_errors_to_reference_tests():
    first = "hgraph_unit_tests/_types/test_metadata.py::test_one"
    second = "hgraph_unit_tests/_types/test_metadata.py::test_two"
    reference = _result({first: _outcome("passed"), second: _outcome("passed")})
    candidate = _result(
        collection_errors=[
            {
                "nodeid": "hgraph_unit_tests/_types/test_metadata.py",
                "outcome": "collection-error",
                "diagnostic": "ImportError: HgTypeMetaData",
            }
        ]
    )

    report = compare_upstream_results(reference, candidate, _manifest())
    assert report["summary"]["review_required"] == 2
    assert {entry["candidate"]["outcome"] for entry in report["review_required"]} == {
        "collection-error"
    }


def test_conformance_reports_reference_failures_as_unverified_not_candidate_gaps():
    nodeid = "hgraph_unit_tests/test_reference.py::test_unstable"
    reference = _result({nodeid: _outcome("failed", "reference defect")})
    candidate = _result({nodeid: _outcome("failed", "candidate also failed")})

    report = compare_upstream_results(reference, candidate, _manifest())
    assert report["summary"]["reference_unverified"] == 1
    assert report["summary"]["review_required"] == 0
    assert report["summary"]["confirmed_gaps"] == 0


def test_conformance_treats_xpass_as_executed_reference_evidence():
    nodeid = "hgraph_unit_tests/test_reference.py::test_xpass"
    reference = _result({nodeid: _outcome("xpassed")})

    report = compare_upstream_results(
        reference, _result({nodeid: _outcome("xpassed")}), _manifest()
    )
    assert report["summary"]["matched"] == 1
    assert report["summary"]["reference_unverified"] == 0


def test_conformance_isolates_only_declared_suite_context_xfails():
    isolated = "hgraph_unit_tests/_operators/test_print.py::test_log_args"
    failed = "hgraph_unit_tests/_operators/test_print.py::test_log_sample"
    reference = _result(
        {
            isolated: _outcome("xfailed", "suite capture pollution"),
            failed: _outcome("failed", "reference defect"),
        }
    )
    rule = {
        "id": "isolated-print",
        "match": "hgraph_unit_tests/_operators/test_print.py::*",
        "classification": "converted",
        "isolate_reference_outcomes": ["xfailed"],
        "candidate_outcomes": ["collection-error"],
        "diagnostic_regex": "private import",
        "reason": "public logging is covered independently",
        "decision": "docs/source/developer_guide/parity_matrix.rst",
        "review_date": "2026-08-05",
        "evidence": ["python/tests/ported/_operators/test_print.py"],
    }

    assert reference_isolation_selectors(reference, _manifest([rule])) == [
        isolated
    ]
    probe = _result({isolated: _outcome("xpassed")})
    record = apply_reference_isolation(reference, isolated, probe)
    assert record["applied"] is True
    assert reference["tests"][isolated]["outcome"] == "xpassed"
    assert reference["tests"][isolated]["suite_result"]["outcome"] == "xfailed"


def test_conformance_keeps_a_failed_isolated_reference_unverified():
    nodeid = "hgraph_unit_tests/_operators/test_print.py::test_log_args"
    reference = _result({nodeid: _outcome("xfailed")})
    record = apply_reference_isolation(
        reference,
        nodeid,
        _result({nodeid: _outcome("xfailed", "still fails alone")}),
    )

    assert record["applied"] is False
    assert reference["tests"][nodeid]["outcome"] == "xfailed"

    report = compare_upstream_results(
        reference,
        _result({nodeid: _outcome("failed", "AssertionError: candidate defect")}),
        _manifest(),
    )
    assert report["summary"]["review_required"] == 0
    assert report["summary"]["reference_unverified"] == 1


def test_conformance_converts_only_an_exact_skipped_reference_reason():
    nodeid = "hgraph_unit_tests/_types/test_cpp_bridge.py::test_type"
    rule = {
        "id": "retired-cpp-bridge",
        "match": "hgraph_unit_tests/_types/test_cpp_bridge.py::*",
        "classification": "converted",
        "reference_outcomes": ["skipped"],
        "reference_diagnostic_regex": "Skipped: C\\+\\+ not enabled",
        "candidate_outcomes": ["collection-error"],
        "diagnostic_regex": "HgTypeMetaData",
        "reason": "the upstream bridge internals are replaced by native type tests",
        "decision": "docs/source/developer_guide/type_reflection.rst",
        "review_date": "2026-08-05",
        "evidence": ["tests/cpp/test_schema_examples.cpp"],
    }
    candidate = _result(
        {nodeid: _outcome("collection-error", "ImportError: HgTypeMetaData")}
    )

    reference = _result(
        {nodeid: _outcome("skipped", "Skipped: C++ not enabled")}
    )
    report = compare_upstream_results(reference, candidate, _manifest([rule]))
    assert report["summary"]["known_expected"] == 1

    reference["tests"][nodeid]["diagnostic"] = "Skipped: missing database"
    report = compare_upstream_results(reference, candidate, _manifest([rule]))
    assert report["summary"]["known_expected"] == 0
    assert report["summary"]["reference_unverified"] == 1


def test_conformance_requires_identical_dependency_versions(monkeypatch):
    import tools.parity.conformance as conformance

    environments = iter(
        [
            {
                "python": "3.14.1",
                "implementation": "CPython",
                "packages": {"pytest": "9.0"},
            },
            {
                "python": "3.14.1",
                "implementation": "CPython",
                "packages": {"pytest": "9.1"},
            },
        ]
    )
    monkeypatch.setattr(
        conformance,
        "conformance_environment",
        lambda interpreter, extras=(): next(environments),
    )

    with pytest.raises(RuntimeError, match="dependencies are not aligned"):
        require_aligned_conformance_environments(Path("reference"), Path("candidate"))


def test_conformance_installs_reference_resolutions_into_candidate(monkeypatch):
    import tools.parity.conformance as conformance

    commands = []
    monkeypatch.setattr(conformance, "_run", lambda command: commands.append(command))
    monkeypatch.setattr(
        conformance,
        "conformance_environment",
        lambda interpreter, extras=(): {
            "packages": {"multimethod": "2.0.2", "pytest": "9.1.1"}
        },
    )

    install_conformance_dependencies((Path("reference"), Path("candidate")))

    assert commands[0][4] == "reference"
    assert commands[1][-2:] == ["multimethod==2.0.2", "pytest==9.1.1"]


def test_manifest_requires_evidence_for_internal_conversions(tmp_path):
    path = tmp_path / "manifest.json"
    manifest = _manifest(
        [
            {
                "id": "unsafe-conversion",
                "match": "hgraph_unit_tests/_types/*",
                "classification": "converted",
                "candidate_outcomes": ["collection-error"],
                "diagnostic_regex": "HgTypeMetaData",
                "reason": "converted",
                "decision": "issue-1",
                "review_date": "2026-08-05",
            }
        ]
    )
    path.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="requires evidence"):
        load_conformance_manifest(path)


def test_manifest_requires_reference_diagnostic_for_skipped_conversion(tmp_path):
    path = tmp_path / "manifest.json"
    manifest = _manifest(
        [
            {
                "id": "unsafe-skipped-conversion",
                "match": "hgraph_unit_tests/_types/*",
                "classification": "converted",
                "reference_outcomes": ["skipped"],
                "candidate_outcomes": ["collection-error"],
                "diagnostic_regex": "HgTypeMetaData",
                "reason": "converted",
                "decision": "docs/source/developer_guide/type_reflection.rst",
                "review_date": "2026-08-05",
                "evidence": ["python/tests/test_reflection.py"],
            }
        ]
    )
    path.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="requires reference_diagnostic_regex"):
        load_conformance_manifest(path)


@pytest.mark.parametrize(
    ("classification", "isolated_outcomes"),
    [("expected-change", ["xfailed"]), ("converted", ["failed"])],
)
def test_manifest_limits_isolated_replay_to_converted_xfails(
    tmp_path, classification, isolated_outcomes
):
    path = tmp_path / "manifest.json"
    manifest = _manifest(
        [
            {
                "id": "unsafe-isolated-replay",
                "match": "hgraph_unit_tests/_operators/test_print.py::*",
                "classification": classification,
                "isolate_reference_outcomes": isolated_outcomes,
                "candidate_outcomes": ["collection-error"],
                "diagnostic_regex": "private import",
                "reason": "public logging is covered independently",
                "decision": "docs/source/developer_guide/parity_matrix.rst",
                "review_date": "2026-08-05",
                "evidence": ["python/tests/ported/_operators/test_print.py"],
            }
        ]
    )
    path.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="only isolate an xfailed converted test"):
        load_conformance_manifest(path)


def test_manifest_profiles_reject_paths_outside_upstream_tests(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "profiles": {"core": ["../hgraph"]},
                "rules": [],
            }
        )
    )
    with pytest.raises(ValueError, match="repository-relative"):
        load_conformance_manifest(path)


def test_profile_selectors_reports_available_profiles():
    with pytest.raises(ValueError, match="choose core"):
        profile_selectors(_manifest(), "missing")


def test_explicit_selectors_cannot_escape_the_upstream_test_tree():
    assert validate_selectors(
        ["hgraph_unit_tests/_operators/test_math.py::test_add"]
    ) == ["hgraph_unit_tests/_operators/test_math.py::test_add"]
    with pytest.raises(ValueError, match="repository-relative"):
        validate_selectors(["../candidate/python/tests"])


def test_staged_upstream_tree_is_exact_and_excludes_the_runtime_package(tmp_path):
    source_root = tmp_path / "source"
    (source_root / "hgraph_unit_tests").mkdir(parents=True)
    (source_root / "examples").mkdir()
    (source_root / "hgraph").mkdir()
    test = source_root / "hgraph_unit_tests" / "test_sample.py"
    test.write_text("def test_sample():\n    assert True\n")
    example = source_root / "examples" / "sample.py"
    example.write_text("VALUE = 1\n")

    import tools.parity.conformance as conformance

    digest = conformance._tree_digest(  # noqa: SLF001 - verifies staging contract
        [source_root / "hgraph_unit_tests", source_root / "examples"],
        root=source_root,
    )
    source = UpstreamSource(
        path=source_root,
        repository="test",
        ref="v_1.0.0",
        revision="test-revision",
        version="1.0.0",
        declared_version="1.0.0",
        test_digest=digest,
    )
    old_root = conformance.PARITY_ROOT
    conformance.PARITY_ROOT = tmp_path / "parity"
    try:
        workspace = prepare_test_workspace(source)
    finally:
        conformance.PARITY_ROOT = old_root

    assert (
        workspace / "hgraph_unit_tests" / "test_sample.py"
    ).read_bytes() == test.read_bytes()
    assert (workspace / "examples" / "sample.py").read_bytes() == example.read_bytes()
    assert not (workspace / "hgraph").exists()


def test_pytest_plugin_records_stable_node_outcomes(tmp_path):
    workspace = tmp_path / "workspace"
    tests = workspace / "hgraph_unit_tests"
    tests.mkdir(parents=True)
    (workspace / "pytest.ini").write_text(
        "[pytest]\naddopts = --import-mode=importlib\n"
    )
    (tests / "test_sample.py").write_text(
        "import pytest\n\n"
        "def test_passes():\n    assert True\n\n"
        "def test_fails():\n    assert 1 == 2\n"
        "\n@pytest.mark.parametrize('value', [1, 2])\n"
        "def test_parameterized(value):\n    assert value > 0\n"
    )
    result_path = tmp_path / "result.json"

    result = run_upstream_suite(
        Path(sys.executable),
        workspace,
        ["hgraph_unit_tests/test_sample.py"],
        result_path=result_path,
        timeout_seconds=30.0,
    )

    assert result["status"] == "complete"
    assert (
        result["tests"]["hgraph_unit_tests/test_sample.py::test_passes"]["outcome"]
        == "passed"
    )
    assert (
        result["tests"]["hgraph_unit_tests/test_sample.py::test_fails"]["outcome"]
        == "failed"
    )
    assert (
        result["tests"]["hgraph_unit_tests/test_sample.py::test_parameterized[case-0]"][
            "outcome"
        ]
        == "passed"
    )
    assert (
        result["tests"]["hgraph_unit_tests/test_sample.py::test_parameterized[case-1]"][
            "outcome"
        ]
        == "passed"
    )
