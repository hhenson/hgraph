"""Fresh-process memory runner for one stable memory profile.

Process measurements intentionally run without GraphDiagnostics: the collector
owns a record for every graph/node it sees and would contaminate RSS.  The
separate ``inspector`` measurement mode reports native planned and dynamic
storage for hg_cpp; its command-line spelling is retained for result-file
compatibility.
"""
import argparse
import gc
import json
import os
import sys
import threading
import time
import traceback

import psutil

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_MIB = 1024 * 1024
_REGISTRY_FIELDS = (
    "node_runtime_types",
    "graph_programs",
    "graph_runtime_types",
    "executor_runtime_types",
    "type_records",
)


def _mb(value: int | float) -> float:
    return round(value / _MIB, 3)


def _implementation_label() -> str:
    import hgraph
    version = getattr(hgraph, "__version__", None)
    if version is not None:
        return str(version)
    try:
        from importlib.metadata import version as distribution_version
        return distribution_version("hgraph")
    except Exception:
        try:
            return "hg_cpp " + distribution_version("hg_cpp")
        except Exception:
            return "unknown"


def _full_memory(process: psutil.Process) -> dict[str, float | None]:
    info = process.memory_info()
    result: dict[str, float | None] = {"rss_mb": _mb(info.rss)}
    try:
        full = process.memory_full_info()
    except (psutil.AccessDenied, AttributeError, OSError):
        result.update(uss_mb=None, pss_mb=None)
    else:
        result["uss_mb"] = _mb(full.uss) if hasattr(full, "uss") else None
        result["pss_mb"] = _mb(full.pss) if hasattr(full, "pss") else None
    return result


def _runtime_registry_snapshot() -> dict[str, int] | None:
    """Capture hg_cpp-only cold-path counts without GraphDiagnostics."""
    try:
        from hgraph.debug import runtime_registry_snapshot
    except (ImportError, AttributeError):
        return None
    snapshot = runtime_registry_snapshot()
    return {
        field: int(getattr(snapshot, field))
        for field in _REGISTRY_FIELDS
    }


class _PeakRssSampler:
    def __init__(self, process: psutil.Process, interval_seconds: float):
        self._process = process
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self.peak_bytes = process.memory_info().rss
        self.samples = 1

    def _sample(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            if not self.observe():
                return

    def observe(self) -> bool:
        """Record an immediate sample; false means the process was unavailable."""
        try:
            self.peak_bytes = max(
                self.peak_bytes, self._process.memory_info().rss
            )
            self.samples += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            return False
        return True

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join()
        self.observe()


def _base_result(profile_id, profile, scenario, process_start) -> dict:
    result = {
        "profile": profile_id,
        "profile_group": profile.group,
        "profile_label": profile.label,
        "growth_axis": profile.growth_axis,
        "expectation": profile.expectation,
        "scenario": profile.scenario,
        "scenario_group": scenario.group,
        "cycle_scale": profile.cycle_scale,
        "size_scale": profile.size_scale,
        "size_step": profile.size_step,
        "use_cpp": os.environ.get("HGRAPH_USE_CPP", ""),
        "source_fingerprint": os.environ.get(
            "HGRAPH_BENCHMARK_SOURCE_FINGERPRINT", ""
        ),
        "hgraph": _implementation_label(),
        "python": ".".join(map(str, sys.version_info[:3])),
        "psutil": psutil.__version__,
        "repetitions": profile.repetitions,
        "process_start_rss_mb": process_start["rss_mb"],
        "process_start_uss_mb": process_start["uss_mb"],
    }
    try:
        import _hgraph
        result["native_module"] = _hgraph.__file__
    except ImportError:
        result["native_module"] = ""
    return result


def run_process(profile_id, profile, scenario, interval_ms: float,
                process_start) -> dict:
    import hgraph as hg

    process = psutil.Process()
    gc.collect()
    ready = _full_memory(process)
    graph_fn, cycles = scenario.build(profile.cycle_scale, profile.size_scale)
    gc.collect()
    pre_run = _full_memory(process)
    pre_run_registry = _runtime_registry_snapshot()
    cycles_per_run = cycles
    total_cycles = 0
    post_gc_series = []
    post_gc_uss_series = []
    registry_series = []
    t0 = time.perf_counter()
    with _PeakRssSampler(process, interval_ms / 1000.0) as sampler:
        for repetition in range(profile.repetitions):
            if repetition and profile.size_step:
                graph_fn, cycles = scenario.build(
                    profile.cycle_scale,
                    profile.size_scale + repetition * profile.size_step,
                )
            start = hg.MIN_ST
            end = start + (cycles + 2) * hg.MIN_TD
            output = hg.run_graph(
                graph_fn, start_time=start, end_time=end, print_progress=False
            )
            post_run = _full_memory(process)
            sampler.observe()  # before teardown can return pages to the OS
            del output
            gc.collect()
            gc.collect()
            checkpoint = _full_memory(process)
            post_gc_series.append(checkpoint["rss_mb"])
            post_gc_uss_series.append(checkpoint["uss_mb"])
            registry_series.append(_runtime_registry_snapshot())
            total_cycles += cycles
    seconds = time.perf_counter() - t0
    post_gc = checkpoint

    peak_mb = _mb(sampler.peak_bytes)
    result = {
        "ok": True,
        "measurement": "process",
        "cycles": total_cycles,
        "cycles_per_run": cycles_per_run,
        "seconds": round(seconds, 6),
        "ready_rss_mb": ready["rss_mb"],
        "ready_uss_mb": ready["uss_mb"],
        "ready_pss_mb": ready["pss_mb"],
        "runtime_load_increment_mb": round(
            ready["rss_mb"] - process_start["rss_mb"], 3
        ),
        "pre_run_rss_mb": pre_run["rss_mb"],
        "pre_run_uss_mb": pre_run["uss_mb"],
        "run_peak_rss_mb": peak_mb,
        "peak_increment_mb": round(peak_mb - pre_run["rss_mb"], 3),
        "post_run_rss_mb": post_run["rss_mb"],
        "post_run_uss_mb": post_run["uss_mb"],
        "post_gc_rss_mb": post_gc["rss_mb"],
        "post_gc_uss_mb": post_gc["uss_mb"],
        "retained_increment_mb": round(
            post_gc["rss_mb"] - pre_run["rss_mb"], 3
        ),
        "retained_uss_increment_mb": (
            round(post_gc["uss_mb"] - pre_run["uss_mb"], 3)
            if post_gc["uss_mb"] is not None and pre_run["uss_mb"] is not None
            else None
        ),
        "post_gc_rss_series_mb": post_gc_series,
        "post_gc_uss_series_mb": post_gc_uss_series,
        "pre_run_registry": pre_run_registry,
        "post_gc_registry_series": registry_series,
        "repeat_growth_mb": round(
            post_gc_series[-1] - post_gc_series[0], 3
        ),
        "repeat_uss_growth_mb": (
            round(post_gc_uss_series[-1] - post_gc_uss_series[0], 3)
            if post_gc_uss_series[-1] is not None
            and post_gc_uss_series[0] is not None else None
        ),
        "sampling_interval_ms": interval_ms,
        "rss_samples": sampler.samples,
    }
    final_registry = registry_series[-1] if registry_series else None
    for field in _REGISTRY_FIELDS:
        result[f"{field}_growth"] = (
            final_registry[field] - pre_run_registry[field]
            if final_registry is not None and pre_run_registry is not None
            else None
        )
    return result


def run_inspector(profile_id, profile, scenario) -> dict:
    import hgraph as hg
    from hgraph.debug import GraphDiagnostics

    diagnostics = GraphDiagnostics(recent_window=1)
    graph_fn, cycles = scenario.build(profile.cycle_scale, profile.size_scale)
    start = hg.MIN_ST
    end = start + (cycles + 2) * hg.MIN_TD
    hg.run_graph(
        graph_fn,
        start_time=start,
        end_time=end,
        print_progress=False,
        life_cycle_observers=[diagnostics],
    )
    snapshot = diagnostics.snapshot()
    entries = sorted(
        snapshot.entries,
        key=lambda entry: entry.peak_storage.dynamic_reserved_bytes,
        reverse=True,
    )
    return {
        "ok": True,
        "measurement": "inspector",
        "cycles": cycles,
        "planned_bytes": snapshot.planned_bytes,
        "dynamic_live_bytes": snapshot.dynamic_live_bytes,
        "dynamic_reserved_bytes": snapshot.dynamic_reserved_bytes,
        "peak_dynamic_live_bytes": snapshot.peak_dynamic_live_bytes,
        "peak_dynamic_reserved_bytes": snapshot.peak_dynamic_reserved_bytes,
        "entry_count": len(snapshot.entries),
        "peak_nested_graph_count": max(
            (entry.peak_storage.nested_graph_count for entry in entries),
            default=0,
        ),
        "peak_nested_graph_capacity": max(
            (entry.peak_storage.nested_graph_capacity for entry in entries),
            default=0,
        ),
        "top_dynamic_entries": [
            {
                "path": entry.path,
                "label": entry.label,
                "implementation": entry.implementation_label,
                "peak_live_bytes": entry.peak_storage.dynamic_live_bytes,
                "peak_reserved_bytes": entry.peak_storage.dynamic_reserved_bytes,
                "peak_nested_graph_count": entry.peak_storage.nested_graph_count,
                "peak_nested_graph_capacity": entry.peak_storage.nested_graph_capacity,
            }
            for entry in entries[:10]
            if entry.peak_storage.dynamic_reserved_bytes
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--measurement", choices=("process", "inspector"),
                        default="process")
    parser.add_argument("--sampling-interval-ms", type=float, default=5.0)
    args = parser.parse_args()
    if args.sampling_interval_ms <= 0:
        parser.error("--sampling-interval-ms must be positive")

    process_start = _full_memory(psutil.Process())

    import memory_profiles
    import scenarios

    profile = memory_profiles.PROFILES[args.profile]
    scenario = scenarios.SCENARIOS[profile.scenario]
    result = _base_result(args.profile, profile, scenario, process_start)
    try:
        if args.measurement == "process":
            result.update(run_process(
                args.profile, profile, scenario, args.sampling_interval_ms,
                process_start,
            ))
        else:
            result.update(run_inspector(args.profile, profile, scenario))
    except Exception:
        result.update(ok=False, measurement=args.measurement,
                      error=traceback.format_exc(limit=30))
    print("@@RESULT@@" + json.dumps(result))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
