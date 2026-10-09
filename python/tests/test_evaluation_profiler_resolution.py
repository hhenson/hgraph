"""The evaluation profiler must accumulate sub-microsecond node evaluations.

Phase intervals used to be cast to the microsecond ``TimeDelta`` per sample
before summing, so a native node costing tens of nanoseconds reported a total
of (almost) zero over tens of thousands of evaluations. Totals now accumulate
in the monotonic clock's own resolution (developer guide, "Evaluation
profiling").
"""
from datetime import timedelta

import hgraph as hg
from hgraph import TS, generator, graph, null_sink
from hgraph.test import EvaluationProfiler

CYCLES = 20_000


@generator
def _pulse(cycles: int) -> TS[int]:
    for i in range(cycles):
        yield hg.MIN_TD, i


@graph
def _native_chain():
    null_sink(_pulse(CYCLES) + 1)


def test_native_node_total_time_accumulates_below_microsecond_samples():
    profiler = EvaluationProfiler()
    start = hg.MIN_ST
    hg.run_graph(_native_chain, start_time=start, end_time=start + (CYCLES + 2) * hg.MIN_TD,
                 print_progress=False, __profile__=profiler)
    snapshot = profiler.snapshot()
    adds = [entry for entry in snapshot.entries
            if not entry.graph and "scalar_add" in entry.path]
    assert adds, [entry.path for entry in snapshot.entries]
    add = adds[0]
    assert add.evaluation.count >= CYCLES
    # A native add evaluation is far below one microsecond; per-sample
    # truncation reported a total near zero here. Even at 5 ns per evaluation
    # the accumulated total is 100 us, so 50 us is a safe floor.
    assert add.evaluation.total_time >= timedelta(microseconds=50), add.evaluation.total_time
    assert add.evaluation.max_time >= add.evaluation.total_time / add.evaluation.count
    assert snapshot.root_evaluation_time >= add.evaluation.total_time
