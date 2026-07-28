"""Exercise mesh_'s sparse child worklist and schedule queue.

The first tick creates every mesh child. Later ticks modify one existing key
at a time while each visited child retains a deadline beyond the run horizon.
The mesh should evaluate and re-rank only the notified child instead of
rebuilding the full live-slot order on every sparse tick.

Run: ``python benchmarks/repro_mesh_schedule_queue.py [keys] [cycles] [repeats]``.
"""

from datetime import timedelta
import sys
import time

import hgraph as hg
from hgraph import TS, TSD, generator, graph


KEYS = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000
CYCLES = int(sys.argv[2]) if len(sys.argv) > 2 else 1_000
REPEATS = int(sys.argv[3]) if len(sys.argv) > 3 else 5


@generator
def values() -> TSD[int, TS[int]]:
    yield hg.MIN_TD, {key: key for key in range(KEYS)}
    for cycle in range(CYCLES):
        key = cycle % KEYS
        yield hg.MIN_TD, {key: KEYS + cycle}


@graph
def scheduled_child(value: TS[int]) -> TS[int]:
    return hg.default(hg.lag(value, timedelta(seconds=1)), 0)


@graph
def app():
    hg.null_sink(hg.mesh_(scheduled_child, values()))


for run in range(REPEATS):
    started = time.perf_counter()
    hg.run_graph(
        app,
        start_time=hg.MIN_ST,
        end_time=hg.MIN_ST + hg.MIN_TD * (CYCLES + 2),
    )
    print(
        f"run={run} keys={KEYS} cycles={CYCLES} "
        f"elapsed={time.perf_counter() - started:.6f}s"
    )
