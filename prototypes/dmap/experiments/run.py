"""The v0 experiment set.

Every experiment is the same assertion in a different shape:

    dmap_(kernel, inputs, workers=N)  ==  map_(kernel, inputs)

RFC 0037 states determinism as the contract that makes the feature worth
having -- distribution changes throughput, never semantics -- so equality
against ``map_`` is the only criterion that matters here. A prototype that is
merely "close" has disproved the design rather than validated it.

Run:  .venv/bin/python prototypes/dmap/experiments/run.py [--workers 1,2,3]
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hgraph import TS, TSD, REMOVE, graph, map_  # noqa: E402
from hgraph.test import eval_node  # noqa: E402

from dmap import dmap_, dmap_shared_  # noqa: E402
from dmap.kernels import KERNELS  # noqa: E402

START = datetime(2020, 1, 1)


# --- the input series each experiment replays ------------------------------
# A list entry is one engine cycle; None means "no tick this cycle".

EXPERIMENTS = {
    # E1: the simplest thing that can work -- stateless, one key set, no
    # removals. Validates partition routing, the delta round trip and output
    # merging, and nothing else.
    "E1_stateless": dict(
        kernel="increment",
        values=[{"a": 1.0}, {"b": 2.0}, {"a": 3.0, "b": 4.0}, {"c": 5.0}],
        shared=None,
    ),
    # E2: per-key state must live in the worker and survive between cycles.
    # This is what separates distributing a map_ from calling a function
    # remotely: get the routing wrong and a key's history lands in two places.
    "E2_stateful_sum": dict(
        kernel="running_sum",
        values=[{"a": 1.0}, {"a": 2.0, "b": 10.0}, {"a": 3.0}, {"b": 20.0}],
        shared=None,
    ),
    # E3: output depends only on a key's tick history, so a dropped or
    # duplicated dispatch shows up immediately.
    "E3_tick_count": dict(
        kernel="tick_count",
        values=[{"a": 1.0, "b": 1.0}, {"a": 1.0}, {"a": 1.0, "b": 1.0}, {"b": 1.0}],
        shared=None,
    ),
    # E4: the broadcast input must reach every worker holding a live key, on
    # the cycle it ticks -- including cycles where nothing else ticks at all.
    "E4_shared_input": dict(
        kernel="scale",
        values=[{"a": 1.0, "b": 2.0}, None, {"c": 3.0}, None],
        shared=[2.0, 10.0, None, 100.0],
    ),
    # E5: keys arriving and leaving mid-run must build and tear down children
    # in the owning worker only.
    "E5_key_lifecycle": dict(
        kernel="running_sum",
        values=[
            {"a": 1.0, "b": 1.0},
            {"c": 1.0},
            {"a": REMOVE, "c": 2.0},
            {"b": 2.0, "a": 5.0},
        ],
        shared=None,
    ),
}


def _reference(kernel_name: str, values, shared):
    kernel = KERNELS[kernel_name]
    if shared is None:

        @graph
        def g(ts: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
            return map_(kernel, ts)

        return eval_node(g, values, __start_time__=START, __elide__=True)

    @graph
    def g(ts: TSD[str, TS[float]], s: TS[float]) -> TSD[str, TS[float]]:
        return map_(kernel, ts, s)

    return eval_node(g, values, shared, __start_time__=START, __elide__=True)


def _distributed(kernel_name: str, values, shared, workers: int):
    if shared is None:

        @graph
        def g(ts: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
            return dmap_(ts, kernel=kernel_name, workers=workers)

        return eval_node(g, values, __start_time__=START, __elide__=True)

    @graph
    def g(ts: TSD[str, TS[float]], s: TS[float]) -> TSD[str, TS[float]]:
        return dmap_shared_(ts, s, kernel=kernel_name, workers=workers)

    return eval_node(g, values, shared, __start_time__=START, __elide__=True)


def _normalise(result):
    """eval_node returns None for a cycle with no tick; make that comparable."""
    return [dict(r) if r else None for r in (result or [])]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", default="1,2,3")
    parser.add_argument("--only", default=None)
    args = parser.parse_args()
    worker_counts = [int(w) for w in args.workers.split(",")]

    failures = []
    for name, spec in EXPERIMENTS.items():
        if args.only and args.only not in name:
            continue
        expected = _normalise(_reference(spec["kernel"], spec["values"], spec["shared"]))
        for workers in worker_counts:
            t0 = time.monotonic()
            actual = _normalise(
                _distributed(spec["kernel"], spec["values"], spec["shared"], workers)
            )
            elapsed = time.monotonic() - t0
            ok = actual == expected
            print(
                f"{'PASS' if ok else 'FAIL'}  {name:<20} workers={workers}  "
                f"{elapsed:6.2f}s",
                flush=True,
            )
            if not ok:
                failures.append((name, workers))
                print(f"        expected: {expected}", flush=True)
                print(f"        actual:   {actual}", flush=True)

    print(flush=True)
    if failures:
        print(f"{len(failures)} FAILED: {failures}", flush=True)
        return 1
    print("all experiments agree with map_", flush=True)
    return 0


if __name__ == "__main__":
    code = main()
    import os

    os._exit(code)
