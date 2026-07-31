"""Regression tests for the memory campaign's stable reporting contract."""

import importlib.util
import sys
from pathlib import Path


_BENCHMARKS = Path(__file__).parents[2] / "benchmarks"
sys.path.insert(0, str(_BENCHMARKS))
_SPEC = importlib.util.spec_from_file_location(
    "hgraph_memory_orchestrator", _BENCHMARKS / "memory_orchestrate.py"
)
memory = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(memory)


def _sample(peak, retained, ready=100.0):
    return {
        "profile": "tick_std__short",
        "ok": True,
        "measurement": "process",
        "cycles": 100,
        "repetitions": 1,
        "process_start_rss_mb": 20.0,
        "process_start_uss_mb": 18.0,
        "ready_rss_mb": ready,
        "ready_uss_mb": ready - 10,
        "ready_pss_mb": ready - 5,
        "runtime_load_increment_mb": ready - 20.0,
        "pre_run_rss_mb": ready,
        "pre_run_uss_mb": ready - 10,
        "run_peak_rss_mb": ready + peak,
        "peak_increment_mb": peak,
        "post_run_rss_mb": ready + retained,
        "post_run_uss_mb": ready - 10 + retained,
        "post_gc_rss_mb": ready + retained,
        "post_gc_uss_mb": ready - 10 + retained,
        "retained_increment_mb": retained,
        "retained_uss_increment_mb": retained,
        "repeat_growth_mb": 0.0,
        "repeat_uss_growth_mb": 0.0,
        "seconds": 1.0,
    }


def test_memory_profiles_reference_stable_scenarios_and_have_growth_context():
    assert memory.DEFAULT_MODES == memory.MODES
    for profile in memory.memory_profiles.PROFILES.values():
        assert profile.scenario in memory.scenarios.SCENARIOS
        assert profile.growth_axis
        assert profile.expectation
        assert profile.cycle_scale > 0
        assert profile.size_scale > 0


def test_memory_samples_use_median_and_preserve_raw_samples():
    result = memory.aggregate_samples([
        _sample(10.0, 3.0),
        _sample(30.0, 9.0),
        _sample(20.0, 6.0),
    ])

    assert result["peak_increment_mb"] == 20.0
    assert result["peak_increment_mb_mad"] == 10.0
    assert result["retained_increment_mb"] == 6.0
    assert result["sample_count"] == 3
    assert len(result["samples"]) == 3


def test_missing_pss_or_uss_is_reported_as_unavailable():
    samples = [_sample(10.0, 3.0), _sample(12.0, 4.0)]
    for sample in samples:
        sample["ready_pss_mb"] = None
        sample["retained_uss_increment_mb"] = None

    result = memory.aggregate_samples(samples)

    assert result["ready_pss_mb"] is None
    assert result["retained_uss_increment_mb"] is None


def test_memory_sample_failure_is_not_hidden():
    result = memory.aggregate_samples([
        _sample(10.0, 3.0),
        {"profile": "tick_std__short", "ok": False, "error": "boom"},
    ])

    assert not result["ok"]
    assert "sample 2: boom" in result["error"]


def test_memory_report_keeps_process_and_inspector_units_distinct():
    python = memory.aggregate_samples([_sample(30.0, 5.0)] * 3)
    legacy = memory.aggregate_samples([_sample(20.0, 4.0)] * 3)
    candidate = memory.aggregate_samples([_sample(10.0, 2.0)] * 3)
    report = memory.render(
        {"tick_std__short": {
            "upstream-py": python,
            "upstream-cpp": legacy,
            "hg-cpp": candidate,
        }},
        {"tick_std__short": {
            "ok": True,
            "planned_bytes": 64 * 1024,
            "peak_dynamic_reserved_bytes": 32 * 1024,
        }},
        samples=3,
        interval_ms=5.0,
    )

    assert "Peak delta" in report
    assert "native-accounted bytes, not RSS" in report
    assert "hg/Python" in report
    assert "hg/hgraph C++" in report
    assert "0.33x" in report
    assert "0.50x" in report
    assert "| 64.0 | 32.0 |" in report


def test_python_reference_remains_reportable_on_demand():
    python = memory.aggregate_samples([_sample(30.0, 5.0)] * 3)
    report = memory.render(
        {"tick_std__short": {"upstream-py": python}},
        {},
        samples=3,
        interval_ms=5.0,
    )

    assert "Python peak delta" in report
    assert "Python retained" in report
    assert "30.0 +/- 0.0" in report
    assert "hg/Python" not in report
    assert "hg/hgraph C++" not in report


def test_process_lifetime_profiles_report_first_to_last_growth():
    legacy_sample = _sample(2.0, 2.0)
    legacy_sample["repeat_growth_mb"] = 1.0
    candidate_sample = _sample(3.0, 3.0)
    candidate_sample["repeat_growth_mb"] = 0.5
    report = memory.render(
        {"construct_std__repeat_ten": {
            "upstream-cpp": memory.aggregate_samples([legacy_sample] * 3),
            "hg-cpp": memory.aggregate_samples([candidate_sample] * 3),
        }},
        {},
        samples=3,
        interval_ms=5.0,
    )

    assert "hgraph C++ first-to-last growth" in report
    assert "hg_cpp first-to-last growth" in report
    assert "1.0 +/- 0.0 | 0.5 +/- 0.0" in report


def test_memory_baseline_cache_requires_exact_identity(tmp_path):
    path = tmp_path / "baseline.json"
    identity = {"schema": 1, "memory_pack": "abc"}
    measured = memory.aggregate_samples([_sample(10.0, 2.0)] * 3)
    memory.save_baseline_cache(
        path, identity, {"tick_std__short": {"upstream-cpp": measured}}
    )

    loaded = memory.load_baseline_cache(path, identity)
    reused = memory.cached_baseline_result(
        loaded, "tick_std__short", "upstream-cpp"
    )

    assert reused["baseline_reused"] is True
    assert memory.load_baseline_cache(
        path, {"schema": 1, "memory_pack": "changed"}
    ) == {}
