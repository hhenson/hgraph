"""Tests for benchmark sampling and report presentation."""

import importlib.util
from pathlib import Path
import subprocess

import pytest


_ORCHESTRATE_PATH = (
    Path(__file__).parents[2] / "benchmarks" / "orchestrate.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "hgraph_benchmark_orchestrator", _ORCHESTRATE_PATH
)
orchestrate = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(orchestrate)


def _sample(seconds, rss=10.0):
    return {
        "scenario": "sample",
        "group": "Readable group",
        "label": "Readable workload",
        "suite": "core",
        "supported_modes": ["upstream-py", "upstream-cpp", "release", "hg-cpp"],
        "ok": True,
        "seconds": seconds,
        "cycles": 100,
        "max_rss_mb": rss,
    }


def test_first_line_accepts_compiler_banner_from_failed_stderr(monkeypatch):
    banner = "Microsoft (R) C/C++ Optimizing Compiler Version 19.51.36256"
    monkeypatch.setattr(
        orchestrate.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], returncode=2, stdout="", stderr=banner + "\n"
        ),
    )

    assert orchestrate._compiler_version(["cl", "--version"]) == banner


def test_first_line_rejects_diagnostics_from_failed_commands(monkeypatch):
    monkeypatch.setattr(
        orchestrate.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], returncode=1, stdout="", stderr="fatal: not a repository\n"
        ),
    )

    assert orchestrate._first_line(["git", "rev-parse", "HEAD"]) == "unknown"
    assert orchestrate._compiler_version(["c++", "--version"]) == "unknown"


def test_first_line_reports_unknown_when_command_has_no_output(monkeypatch):
    monkeypatch.setattr(
        orchestrate.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], returncode=1, stdout="", stderr=""
        ),
    )

    assert orchestrate._first_line(["missing", "--version"]) == "unknown"


def test_public_benchmark_artifacts_redact_developer_local_paths(
    monkeypatch, tmp_path
):
    repo = tmp_path / "checkout"
    home = tmp_path / "developer"
    monkeypatch.setattr(orchestrate, "REPO_ROOT", repo)
    monkeypatch.setenv("HOME", str(home))

    sanitized = orchestrate.sanitize_public_artifact({
        "native_module": str(repo / ".venv" / "_hgraph.so"),
        "error": f"failed in {home / 'private' / 'source.cpp'}",
        "samples": [{"native_module": "/opt/hgraph/_hgraph.so"}],
    })

    assert sanitized["native_module"] == "<repo>/.venv/_hgraph.so"
    assert sanitized["error"] == "failed in <home>/private/source.cpp"
    assert sanitized["samples"][0]["native_module"] == "_hgraph.so"

    monkeypatch.setattr(
        orchestrate, "REPO_ROOT", orchestrate.PureWindowsPath(r"C:\checkout")
    )
    assert orchestrate._portable_native_module(
        r"C:\checkout\benchmarks\_hgraph.pyd"
    ) == "<repo>/benchmarks/_hgraph.pyd"


def test_benchmark_samples_use_median_and_report_spread():
    result = orchestrate.aggregate_samples(
        [_sample(1.0, 10.0), _sample(9.0, 30.0), _sample(2.0, 20.0)]
    )

    assert result["seconds"] == 2.0
    assert result["seconds_mad"] == 1.0
    assert result["seconds_min"] == 1.0
    assert result["seconds_max"] == 9.0
    assert result["max_rss_mb"] == 30.0
    assert result["sample_count"] == 3


def test_benchmark_report_groups_readable_names_and_marks_unsupported_modes():
    measured = orchestrate.aggregate_samples(
        [_sample(1.0), _sample(1.2), _sample(0.8)]
    )
    skipped = {
        "scenario": "sample",
        "group": "Readable group",
        "label": "Readable workload",
        "suite": "core",
        "skipped": True,
    }
    report = orchestrate.render(
        {"sample": {"upstream-py": skipped, "hg-cpp": measured}},
        cycle_scale=1.0,
        size_scale=2.0,
        samples=3,
    )

    assert "## Readable group" in report
    assert "Readable workload (`sample`)" in report
    assert "N/A" in report
    assert "FAIL" not in report
    assert "+/- 0.200s" in report


def test_default_benchmark_report_compares_fixed_release_with_current_source():
    release = orchestrate.aggregate_samples(
        [_sample(2.0), _sample(2.0), _sample(2.0)]
    )
    candidate = orchestrate.aggregate_samples(
        [_sample(1.0), _sample(1.0), _sample(1.0)]
    )

    report = orchestrate.render(
        {"sample": {"release": release, "hg-cpp": candidate}},
        cycle_scale=1.0,
        size_scale=1.0,
        samples=3,
    )

    assert orchestrate.DEFAULT_MODES == ("release", "hg-cpp")
    assert "| workload | cycles | hgraph 0.8.1 | current source |" in report
    assert "speed-up vs hgraph 0.8.1" in report
    assert "1.000s +/- 0.000s (x2.0)" in report
    assert "`upstream-py`" not in report


def test_upstream_baseline_cache_reuses_only_matching_successes(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(orchestrate, "RESULTS_DIR", tmp_path)
    cache_path = tmp_path / "nested" / "baseline.json"
    identity = {"schema": 1, "upstream_hgraph": "1.2.3"}
    measured = orchestrate.aggregate_samples(
        [_sample(2.0), _sample(2.1), _sample(1.9)]
    )
    measured["benchmark_metadata"] = {"revision": "obsolete"}
    results = {"sample": {"upstream-cpp": measured}}
    orchestrate.save_baseline_cache(cache_path, identity, results)

    loaded = orchestrate.load_baseline_cache(cache_path, identity)
    reused = orchestrate.cached_baseline_result(
        loaded, "sample", "upstream-cpp"
    )

    assert reused["seconds"] == 2.0
    assert reused["baseline_reused"] is True
    assert "benchmark_metadata" not in reused
    assert "baseline_reused" not in loaded["sample"]["upstream-cpp"]
    assert orchestrate.load_baseline_cache(
        cache_path, {**identity, "upstream_hgraph": "2.0.0"}
    ) == {}
    assert orchestrate.cached_baseline_result(
        loaded, "missing", "upstream-cpp"
    ) is None


def test_baseline_cache_path_cannot_escape_results_directory(
    monkeypatch, tmp_path
):
    results_dir = tmp_path / "results"
    monkeypatch.setattr(orchestrate, "RESULTS_DIR", results_dir)

    inside = results_dir / "controlled" / "baseline.json"
    orchestrate.save_baseline_cache(inside, {"schema": 1}, {})
    assert inside.exists()

    with pytest.raises(ValueError, match="must be inside"):
        orchestrate.save_baseline_cache(
            tmp_path / "outside.json", {"schema": 1}, {}
        )


def test_baseline_identity_is_fixed_to_both_released_lines():
    identity = orchestrate.baseline_identity(1.0, 1.0, 5, ["release"])

    assert identity["hgraph_versions"] == {
        "upstream-py": "0.5.41",
        "upstream-cpp": "0.5.41",
        "release": "0.8.1",
    }
    assert identity["fixed_release_artifact"] == (
        orchestrate.fixed_release_artifact()["sha256"]
    )
    assert identity["reference_artifact"] == (
        orchestrate.reference_artifact()["sha256"]
    )


def test_fixed_release_invocation_records_exact_artifact(monkeypatch):
    monkeypatch.setattr(
        orchestrate,
        "fixed_release_artifact",
        lambda: {"sha256": "abc123"},
    )

    _, environment = orchestrate.mode_invocation("release")

    assert environment == {
        "HGRAPH_BENCHMARK_FIXED_RELEASE": "0.8.1",
        "HGRAPH_BENCHMARK_FIXED_RELEASE_SHA256": "abc123",
    }


def test_upstream_environment_installs_pinned_python_first_0_5_artifact(
    monkeypatch, tmp_path
):
    calls = []
    monkeypatch.setattr(orchestrate, "UPSTREAM_VENV", tmp_path / "upstream")
    monkeypatch.setattr(
        orchestrate,
        "UPSTREAM_ARTIFACT_FILE",
        tmp_path / "upstream" / ".artifact-sha256",
    )
    monkeypatch.setattr(
        orchestrate,
        "reference_artifact",
        lambda: {
            "filename": "hgraph-0.5.41.whl",
            "url": "https://example.test/hgraph-0.5.41.whl",
            "sha256": "ref123",
        },
    )
    monkeypatch.setattr(
        orchestrate, "installed_hgraph_version", lambda _python: "0.5.41"
    )
    monkeypatch.setattr(
        orchestrate.subprocess,
        "run",
        lambda command, check: calls.append(command),
    )
    orchestrate.UPSTREAM_VENV.mkdir()

    orchestrate.ensure_upstream_venv()

    assert calls[-1][-2:] == [
        "--reinstall",
        "https://example.test/hgraph-0.5.41.whl#sha256=ref123",
    ]
    assert orchestrate.UPSTREAM_ARTIFACT_FILE.read_text().strip() == "ref123"


def test_upstream_environment_replaces_a_post_port_hgraph_release(
    monkeypatch, tmp_path
):
    calls = []
    upstream = tmp_path / "upstream"
    monkeypatch.setattr(orchestrate, "UPSTREAM_VENV", upstream)
    monkeypatch.setattr(
        orchestrate,
        "UPSTREAM_ARTIFACT_FILE",
        upstream / ".artifact-sha256",
    )
    monkeypatch.setattr(
        orchestrate,
        "reference_artifact",
        lambda: {
            "filename": "hgraph-0.5.41.whl",
            "url": "https://example.test/hgraph-0.5.41.whl",
            "sha256": "ref123",
        },
    )
    python = orchestrate.upstream_python()
    python.parent.mkdir(parents=True)
    python.touch()
    versions = iter(("0.8.0", "0.5.41"))
    monkeypatch.setattr(
        orchestrate, "installed_hgraph_version", lambda _python: next(versions)
    )
    monkeypatch.setattr(
        orchestrate.subprocess,
        "run",
        lambda command, check: calls.append(command),
    )

    orchestrate.ensure_upstream_venv()

    assert calls == [[
        "uv", "pip", "install", "--python", str(python), "--reinstall",
        "https://example.test/hgraph-0.5.41.whl#sha256=ref123",
    ]]


def test_fixed_release_environment_installs_published_0_8_1(
    monkeypatch, tmp_path
):
    calls = []
    monkeypatch.setattr(orchestrate, "RELEASE_VENV", tmp_path / "release")
    monkeypatch.setattr(
        orchestrate,
        "RELEASE_ARTIFACT_FILE",
        tmp_path / "release" / ".artifact-sha256",
    )
    monkeypatch.setattr(
        orchestrate,
        "fixed_release_artifact",
        lambda: {
            "filename": "hgraph-0.8.1.whl",
            "url": "https://example.test/hgraph-0.8.1.whl",
            "sha256": "abc123",
        },
    )
    monkeypatch.setattr(
        orchestrate, "installed_hgraph_version", lambda _python: "0.8.1"
    )
    monkeypatch.setattr(
        orchestrate.subprocess,
        "run",
        lambda command, check: calls.append(command),
    )
    orchestrate.RELEASE_VENV.mkdir()

    orchestrate.ensure_release_venv()

    assert calls[-1][-2:] == [
        "--reinstall", "https://example.test/hgraph-0.8.1.whl#sha256=abc123"
    ]
    assert str(orchestrate.release_python()) in calls[-1]
    assert orchestrate.RELEASE_ARTIFACT_FILE.read_text().strip() == "abc123"


def test_benchmark_sample_failure_is_not_hidden_by_successful_samples():
    failed = orchestrate.aggregate_samples(
        [_sample(1.0), {"scenario": "sample", "ok": False, "error": "boom"}]
    )

    assert not failed["ok"]
    assert "sample 2: boom" in failed["error"]


def test_cpp_first_only_report_section_does_not_claim_a_0_5_comparison():
    measured = orchestrate.aggregate_samples([{
        **_sample(1.0),
        "group": "C++-first - dynamic TSL",
        "label": "Dynamic list workload",
        "supported_modes": ["release", "hg-cpp"],
    }])
    report = orchestrate.render(
        {"dynamic": {"hg-cpp": measured}},
        cycle_scale=1.0,
        size_scale=1.0,
        samples=1,
    )

    section = report.split("## C++-first - dynamic TSL", 1)[1]
    assert "not a cross-implementation comparison" in section
    assert "| workload | cycles | current source |" in section
    assert "upstream-py" not in section
