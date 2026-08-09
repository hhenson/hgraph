"""Portability checks for the fresh-process benchmark runner."""

import importlib.util
from pathlib import Path


_RUNNER_PATH = Path(__file__).parents[2] / "benchmarks" / "runner.py"
_SPEC = importlib.util.spec_from_file_location("hgraph_benchmark_runner", _RUNNER_PATH)
runner = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(runner)


def test_benchmark_runner_reports_peak_resident_memory():
    assert runner._max_rss_mb() > 0.0
