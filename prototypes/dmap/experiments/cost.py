"""What dmap_ costs, so the choice can be made on evidence.

``dmap_`` is not an optimisation to apply by default. Every cycle pays for
pickling a delta, two pipe traversals per worker with work, and a barrier on
the slowest worker. Below some amount of per-key work that cost dominates and
``dmap_`` is simply slower than ``map_``.

This measures the crossover rather than asserting one. It sweeps three axes:

    keys      -- how many children exist
    spin      -- how much work each child does per tick
    workers   -- how many processes share them

and reports dmap_ wall time as a ratio of map_ wall time. Ratio < 1 means
distribution paid for itself on this machine, for this shape.

Raw results are written next to the summary as JSON, because a benchmark
without its raw numbers is not evidence.

Run:  .venv/bin/python prototypes/dmap/experiments/cost.py --out results.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hgraph import TS, TSD, graph, map_  # noqa: E402
from hgraph.test import eval_node  # noqa: E402

from dmap import dmap_  # noqa: E402
from dmap.kernels import KERNELS  # noqa: E402

START = datetime(2020, 1, 1)


def _series(keys: int, cycles: int):
    """Every key ticks on every cycle: the best case for distribution."""
    names = [f"k{i:05d}" for i in range(keys)]
    return [{name: float(c) for name in names} for c in range(cycles)]


def _time_map(keys: int, cycles: int) -> float:
    kernel = KERNELS["busy"]

    @graph
    def g(ts: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        return map_(kernel, ts)

    data = _series(keys, cycles)
    t0 = time.monotonic()
    eval_node(g, data, __start_time__=START, __elide__=True)
    return time.monotonic() - t0


def _time_dmap(keys: int, cycles: int, workers: int) -> float:
    @graph
    def g(ts: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        return dmap_(ts, kernel="busy", workers=workers)

    data = _series(keys, cycles)
    t0 = time.monotonic()
    eval_node(g, data, __start_time__=START, __elide__=True)
    return time.monotonic() - t0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keys", default="16,64,256")
    parser.add_argument("--spin", default="0,1000,10000")
    parser.add_argument("--workers", default="1,2,4")
    parser.add_argument("--cycles", type=int, default=50)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    rows = []
    print(
        f"{'keys':>6} {'spin':>7} {'map_ s':>9} {'workers':>8} "
        f"{'dmap_ s':>9} {'ratio':>7}",
        flush=True,
    )
    for keys in (int(k) for k in args.keys.split(",")):
        for spin in (int(s) for s in args.spin.split(",")):
            # The worker inherits this through the environment at spawn, so
            # both sides do the same amount of work per tick.
            os.environ["DMAP_SPIN"] = str(spin)
            base = _time_map(keys, args.cycles)
            for workers in (int(w) for w in args.workers.split(",")):
                got = _time_dmap(keys, args.cycles, workers)
                ratio = got / base if base else float("inf")
                rows.append(
                    dict(
                        keys=keys,
                        spin=spin,
                        workers=workers,
                        cycles=args.cycles,
                        map_seconds=base,
                        dmap_seconds=got,
                        ratio=ratio,
                    )
                )
                print(
                    f"{keys:>6} {spin:>7} {base:>9.3f} {workers:>8} "
                    f"{got:>9.3f} {ratio:>7.2f}",
                    flush=True,
                )

    if args.out:
        payload = dict(
            host=platform.node(),
            platform=platform.platform(),
            python=platform.python_version(),
            cpu_count=os.cpu_count(),
            recorded=datetime.now().isoformat(timespec="seconds"),
            rows=rows,
        )
        Path(args.out).write_text(json.dumps(payload, indent=2) + "\n")
        print(f"\nraw results -> {args.out}", flush=True)

    best = min(rows, key=lambda r: r["ratio"]) if rows else None
    if best:
        print(
            f"\nbest ratio {best['ratio']:.2f} at keys={best['keys']} "
            f"spin={best['spin']} workers={best['workers']}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    code = main()
    os._exit(code)
