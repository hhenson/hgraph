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
import resource
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


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

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result.update(
            ok=True,
            seconds=round(seconds, 6),
            cycles=cycles,
            cycles_per_s=round(cycles / seconds) if seconds > 0 else None,
            max_rss_mb=round(rss / (1024 * 1024 if sys.platform == "darwin" else 1024), 1),
        )
    except Exception:
        result.update(ok=False, error=traceback.format_exc(limit=20))
    print("@@RESULT@@" + json.dumps(result))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
