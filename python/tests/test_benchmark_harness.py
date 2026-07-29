"""Tests for benchmark sampling and report presentation."""

import importlib.util
from pathlib import Path


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
        "supported_modes": ["upstream-py", "upstream-cpp", "hg-cpp"],
        "ok": True,
        "seconds": seconds,
        "cycles": 100,
        "max_rss_mb": rss,
    }


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


def test_default_benchmark_report_compares_legacy_cpp_with_hg_cpp():
    legacy = orchestrate.aggregate_samples(
        [_sample(2.0), _sample(2.0), _sample(2.0)]
    )
    candidate = orchestrate.aggregate_samples(
        [_sample(1.0), _sample(1.0), _sample(1.0)]
    )

    report = orchestrate.render(
        {"sample": {"upstream-cpp": legacy, "hg-cpp": candidate}},
        cycle_scale=1.0,
        size_scale=1.0,
        samples=3,
    )

    assert orchestrate.DEFAULT_MODES == ("upstream-cpp", "hg-cpp")
    assert "| workload | cycles | legacy C++ | hg_cpp |" in report
    assert "speed-up vs legacy C++" in report
    assert "1.000s +/- 0.000s (x2.0)" in report
    assert "`upstream-py`" not in report


def test_upstream_baseline_cache_reuses_only_matching_successes(tmp_path):
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


def test_benchmark_sample_failure_is_not_hidden_by_successful_samples():
    failed = orchestrate.aggregate_samples(
        [_sample(1.0), {"scenario": "sample", "ok": False, "error": "boom"}]
    )

    assert not failed["ok"]
    assert "sample 2: boom" in failed["error"]


def test_hg_cpp_only_report_section_does_not_claim_an_upstream_comparison():
    measured = orchestrate.aggregate_samples([{
        **_sample(1.0),
        "group": "hg_cpp - dynamic TSL",
        "label": "Dynamic list workload",
        "supported_modes": ["hg-cpp"],
    }])
    report = orchestrate.render(
        {"dynamic": {"hg-cpp": measured}},
        cycle_scale=1.0,
        size_scale=1.0,
        samples=1,
    )

    section = report.split("## hg_cpp - dynamic TSL", 1)[1]
    assert "not a cross-implementation comparison" in section
    assert "| workload | cycles | hg-cpp |" in section
    assert "upstream-py" not in section
