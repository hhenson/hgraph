"""Measure process-hosted spawn throughput including wiring, startup and drain.

Run with an installed candidate wheel:
    python benchmarks/spawn_pipeline.py > spawn-scaling.csv

Each case runs in a fresh parent process. Peak RSS columns describe the parent
and largest reaped worker separately, not aggregate pipeline memory. CPU cases
compare synchronous composition with two process-hosted Python compute stages.
"""
import argparse
import csv
import json
import statistics
import subprocess
import sys
import time
import tempfile
from pathlib import Path
import os


def peak_rss_mib():
    """Report parent and largest reaped worker RSS where resource is available."""
    if sys.platform == "win32":
        return None, None
    import resource
    scale = 1024 ** 2 if sys.platform == "darwin" else 1024
    parent = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    child = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return round(parent / scale, 2), round(child / scale, 2)


def measure(kind, ticks, stages, width):
    import hgraph as hg

    from spawn_workload import identity, cpu_stage, consume
    directory = tempfile.TemporaryDirectory()
    path = str(Path(directory.name) / "result.json")
    schema = hg.TSD[int, hg.TS[int]] if kind == "dictionary" else hg.TS[int]
    events = ([dict.fromkeys(range(width), 0)] + [{i % width: i} for i in range(1, ticks)]
              if kind == "dictionary" else list(range(ticks)))

    @hg.graph
    def app(value: schema) -> None:
        if kind == "window":
            value = hg.to_window(value, width, 1)
        if kind.startswith("cpu"):
            compute = hg.bind_(cpu_stage, iterations=width)
            if kind == "cpu-sync":
                consume(cpu_stage(cpu_stage(value, width), width), path)
            else:
                hg.spawn_(hg.pipeline_([compute, compute, hg.bind_(consume, path=path)]), value,
                          __capacity_frames__=8, __capacity_bytes__=1024 * 1024)
        else:
            hg.spawn_(hg.pipeline_([identity] * (stages - 1) + [hg.bind_(consume, path=path)]), value,
                      __capacity_frames__=8, __capacity_bytes__=1024 * 1024)

    samples = []
    for _ in range(3):
        start = time.perf_counter()
        hg.eval_node(app, events)
        samples.append(time.perf_counter() - start)
        result = json.loads(Path(path).read_text())
        assert result["count"] == ticks, result
        assert (result["pid"] == os.getpid()) == (kind == "cpu-sync")
    rss_mib, child_rss_mib = peak_rss_mib()
    directory.cleanup()
    return [kind, ticks, stages, width, round(statistics.median(samples), 6), rss_mib, child_rss_mib]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", nargs=4, metavar=("KIND", "TICKS", "STAGES", "WIDTH"))
    args = parser.parse_args()
    if args.case:
        kind, ticks, stages, width = args.case
        print(json.dumps(measure(kind, int(ticks), int(stages), int(width))))
        return
    cases = [("scalar", ticks, stages, 1) for stages in (1, 4) for ticks in (1000, 4000)]
    cases += [(kind, 1000, 2, width) for kind in ("dictionary", "window") for width in (16, 1024)]
    cases += [(kind, 32, 3, 500000) for kind in ("cpu-sync", "cpu-process")]
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(["kind", "ticks", "stages", "width", "median_seconds", "parent_peak_rss_mib", "largest_worker_peak_rss_mib"])
    for case in cases:
        completed = subprocess.run([sys.executable, __file__, "--case", *map(str, case)],
                                   check=True, capture_output=True, text=True, timeout=180)
        writer.writerow(json.loads(completed.stdout.strip().splitlines()[-1]))
        sys.stdout.flush()


if __name__ == "__main__":
    main()
