"""Inner benchmark runner — executes ONE scenario in whichever hgraph
implementation is installed in the current interpreter and prints a JSON
result line. Always run in a fresh process (state isolation; a crash must
not take the matrix down).

    python benchmarks/runner.py --scenario tick_std --cycle-scale 1.0
    python benchmarks/runner.py --list
"""
import argparse
import json
import os
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _max_rss_mb() -> float:
    """Return peak resident memory using the platform's process API."""
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("page_fault_count", wintypes.DWORD),
                ("peak_working_set_size", ctypes.c_size_t),
                ("working_set_size", ctypes.c_size_t),
                ("quota_peak_paged_pool_usage", ctypes.c_size_t),
                ("quota_paged_pool_usage", ctypes.c_size_t),
                ("quota_peak_non_paged_pool_usage", ctypes.c_size_t),
                ("quota_non_paged_pool_usage", ctypes.c_size_t),
                ("pagefile_usage", ctypes.c_size_t),
                ("peak_pagefile_usage", ctypes.c_size_t),
            ]

        get_current_process = ctypes.windll.kernel32.GetCurrentProcess
        get_current_process.restype = wintypes.HANDLE
        get_process_memory_info = ctypes.windll.psapi.GetProcessMemoryInfo
        get_process_memory_info.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        get_process_memory_info.restype = wintypes.BOOL

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        if not get_process_memory_info(
            get_current_process(), ctypes.byref(counters), counters.cb
        ):
            raise ctypes.WinError()
        rss_bytes = counters.peak_working_set_size
    else:
        import resource

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_bytes = rss if sys.platform == "darwin" else rss * 1024
    return round(rss_bytes / (1024 * 1024), 1)


def _implementation_label():
    import hgraph
    version = getattr(hgraph, "__version__", None)
    if version is None:
        try:
            from importlib.metadata import version as _v
            version = _v("hgraph")
        except Exception:
            try:
                from importlib.metadata import version as _v
                version = "hg_cpp " + _v("hg_cpp")
            except Exception:
                version = "unknown"
    return version


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario")
    parser.add_argument("--scale", type=float,
                        help="legacy shorthand setting both cycle and size scale")
    parser.add_argument("--cycle-scale", type=float)
    parser.add_argument("--size-scale", type=float)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    import scenarios as sc

    if args.list:
        for scenario_id, scenario in sc.SCENARIOS.items():
            modes = ",".join(scenario.modes)
            print(
                f"{scenario_id:44} [{scenario.suite}] "
                f"{scenario.group} / {scenario.label} ({modes})"
            )
        return 0

    cycle_scale = args.cycle_scale if args.cycle_scale is not None else args.scale
    size_scale = args.size_scale if args.size_scale is not None else args.scale
    cycle_scale = 1.0 if cycle_scale is None else cycle_scale
    size_scale = 1.0 if size_scale is None else size_scale
    scenario = sc.SCENARIOS[args.scenario]

    result = {
        "scenario": args.scenario,
        "group": scenario.group,
        "label": scenario.label,
        "suite": scenario.suite,
        "supported_modes": list(scenario.modes),
        "cycle_scale": cycle_scale,
        "size_scale": size_scale,
        "use_cpp": os.environ.get("HGRAPH_USE_CPP", ""),
        "source_fingerprint": os.environ.get("HGRAPH_BENCHMARK_SOURCE_FINGERPRINT", ""),
        "fixed_release": os.environ.get("HGRAPH_BENCHMARK_FIXED_RELEASE", ""),
        "fixed_release_sha256": os.environ.get(
            "HGRAPH_BENCHMARK_FIXED_RELEASE_SHA256", ""
        ),
        "reference": os.environ.get("HGRAPH_BENCHMARK_REFERENCE", ""),
        "reference_sha256": os.environ.get(
            "HGRAPH_BENCHMARK_REFERENCE_SHA256", ""
        ),
        "hgraph": _implementation_label(),
        "python": ".".join(map(str, sys.version_info[:3])),
    }
    try:
        import _hgraph
        result["native_module"] = _hgraph.__file__
    except ImportError:
        result["native_module"] = ""
    try:
        import hgraph as hg

        graph_fn, cycles = scenario.build(cycle_scale, size_scale)
        start = hg.MIN_ST
        end = start + (cycles + 2) * hg.MIN_TD

        t0 = time.perf_counter()
        hg.run_graph(graph_fn, start_time=start, end_time=end)
        seconds = time.perf_counter() - t0

        result.update(
            ok=True,
            seconds=round(seconds, 6),
            cycles=cycles,
            cycles_per_s=round(cycles / seconds) if seconds > 0 else None,
            max_rss_mb=_max_rss_mb(),
        )
    except Exception:
        result.update(ok=False, error=traceback.format_exc(limit=20))
    print("@@RESULT@@" + json.dumps(result))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
