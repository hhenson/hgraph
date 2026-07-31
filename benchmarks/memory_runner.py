"""Fresh-process memory runner for one stable memory profile.

Process measurements intentionally run without Inspector: an Inspector owns a
record for every graph/node it sees and would contaminate RSS.  The separate
``inspector`` pass reports native planned and dynamic storage for hg_cpp.
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
            try:
                self.peak_bytes = max(
                    self.peak_bytes, self._process.memory_info().rss
                )
                self.samples += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                return

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join()
        try:
            self.peak_bytes = max(
                self.peak_bytes, self._process.memory_info().rss
            )
            self.samples += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            pass


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
        "use_cpp": os.environ.get("HGRAPH_USE_CPP", ""),
        "source_fingerprint": os.environ.get(
            "HGRAPH_BENCHMARK_SOURCE_FINGERPRINT", ""
        ),
        "hgraph": _implementation_label(),
        "python": ".".join(map(str, sys.version_info[:3])),
        "psutil": psutil.__version__,
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
    start = hg.MIN_ST
    end = start + (cycles + 2) * hg.MIN_TD

    t0 = time.perf_counter()
    with _PeakRssSampler(process, interval_ms / 1000.0) as sampler:
        output = hg.run_graph(
            graph_fn, start_time=start, end_time=end, print_progress=False
        )
    seconds = time.perf_counter() - t0
    post_run = _full_memory(process)
    del output, graph_fn
    gc.collect()
    gc.collect()
    post_gc = _full_memory(process)

    peak_mb = _mb(sampler.peak_bytes)
    return {
        "ok": True,
        "measurement": "process",
        "cycles": cycles,
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
        "sampling_interval_ms": interval_ms,
        "rss_samples": sampler.samples,
    }


def run_inspector(profile_id, profile, scenario) -> dict:
    import hgraph as hg
    from hgraph.debug import Inspector

    inspector = Inspector(recent_window=1)
    graph_fn, cycles = scenario.build(profile.cycle_scale, profile.size_scale)
    start = hg.MIN_ST
    end = start + (cycles + 2) * hg.MIN_TD
    hg.run_graph(
        graph_fn,
        start_time=start,
        end_time=end,
        print_progress=False,
        life_cycle_observers=[inspector],
    )
    snapshot = inspector.snapshot()
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
