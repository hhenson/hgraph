"""Exercise map_'s child-schedule queue under repeated broadcast ticks.

Each mapped child owns one deadline beyond the run horizon while a broadcast
input visits every child on every cycle. The queue should retain one current
deadline per child rather than one duplicate per ``(child, cycle)``.

Run: ``python benchmarks/repro_map_schedule_queue.py [keys] [cycles] [repeats]``.
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


@generator
def pulse() -> TS[int]:
    for value in range(CYCLES):
        yield hg.MIN_TD, value


@graph
def scheduled_child(value: TS[int], broadcast: TS[int]) -> TS[int]:
    delayed = hg.lag(value, timedelta(seconds=1))
    return hg.default(delayed, 0) + broadcast


@graph
def app():
    hg.null_sink(hg.map_(scheduled_child, values(), pulse()))


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
