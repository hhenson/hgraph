"""Measure thread-hosted spawn scaling, including wiring, drain and Python GIL cost.

Run with an installed candidate wheel:
    python benchmarks/spawn_pipeline.py > spawn-scaling.csv

Each case runs in a fresh subprocess; peak RSS includes Python, loaded libraries,
graph state and the input fixture, not just channel payloads. These are scaling
observations, not a claim that spawning trivial work improves throughput.
"""
import argparse
import csv
import json
import statistics
import subprocess
import sys
import time


def measure(kind, ticks, stages, width):
    import hgraph as hg

    received = []

    @hg.graph
    def identity(value: hg.TIME_SERIES_TYPE) -> hg.TIME_SERIES_TYPE:
        return value

    @hg.sink_node
    def consume(value: hg.TIME_SERIES_TYPE):
        received.append(1)

    schema = hg.TSD[int, hg.TS[int]] if kind == "dictionary" else hg.TS[int]
    events = ([{i: 0 for i in range(width)}] + [{i % width: i} for i in range(1, ticks)]
              if kind == "dictionary" else list(range(ticks)))

    @hg.graph
    def app(value: schema) -> None:
        if kind == "window":
            value = hg.to_window(value, width, 1)
        hg.spawn_(hg.pipeline_([identity] * (stages - 1) + [consume]), value,
                  __capacity_frames__=8, __capacity_bytes__=1024 * 1024)

    samples = []
    for _ in range(3):
        received.clear()
        start = time.perf_counter()
        hg.eval_node(app, events)
        samples.append(time.perf_counter() - start)
        assert len(received) == ticks, (len(received), ticks)
    rss_mib = None
    if sys.platform != "win32":
        import resource
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_mib = round(rss / (1024 ** 2 if sys.platform == "darwin" else 1024), 2)
    return [kind, ticks, stages, width, round(statistics.median(samples), 6), rss_mib]


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
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(["kind", "ticks", "stages", "width", "median_seconds", "peak_rss_mib"])
    for case in cases:
        completed = subprocess.run([sys.executable, __file__, "--case", *map(str, case)],
                                   check=True, capture_output=True, text=True, timeout=180)
        writer.writerow(json.loads(completed.stdout.strip().splitlines()[-1]))
        sys.stdout.flush()


if __name__ == "__main__":
    main()
