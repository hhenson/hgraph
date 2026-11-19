"""Measure the first published tick of a map that returns a structural bundle.

Run in a fresh process for each sample, using the same interpreter and CPU
for both hgraph builds. Wiring, root startup and value verification happen
outside the timed interval.
"""
import argparse
import json
import logging
from time import perf_counter_ns

from hgraph import (
    MIN_ST, MIN_TD, TS, TSB, TSD, TimeSeriesSchema, compute_node, generator,
    graph, map_, run_graph, sink_node,
)


class Reading(TimeSeriesSchema):
    a: TS[int]
    b: TS[int]
    c: TS[int]
    d: TS[int]


class ReadingPair(TimeSeriesSchema):
    forward: TSB[Reading]
    reverse: TSB[Reading]


@compute_node
def fields(value: TS[int]) -> TSB[Reading]:
    n = value.value
    return {"a": n, "b": n + 1, "c": n + 2, "d": n + 3}


@graph
def project(value: TS[int]) -> TSB[ReadingPair]:
    row = fields(value)
    reverse = TSB[Reading].from_ts(a=row.d, b=row.c, c=row.b, d=row.a)
    return TSB[ReadingPair].from_ts(forward=row, reverse=reverse)


def run(keys):
    batch = {n: n for n in range(keys)}
    started = 0
    elapsed = None

    @generator
    def trigger() -> TS[bool]:
        yield MIN_TD, True

    @compute_node
    def publish(trigger: TS[bool]) -> TSD[int, TS[int]]:
        nonlocal started
        started = perf_counter_ns()
        return batch

    @sink_node
    def record(result: TSD[int, TSB[ReadingPair]]):
        nonlocal elapsed
        duration = perf_counter_ns() - started
        assert elapsed is None
        assert set(result.keys()) == batch.keys()
        for key, pair in result.items():
            expected = tuple(range(key, key + 4))
            for row, values in ((pair.forward, expected), (pair.reverse, expected[::-1])):
                assert (row.a.value, row.b.value, row.c.value, row.d.value) == values
        elapsed = duration

    @graph
    def scenario():
        record(map_(project, publish(trigger())))

    run_graph(scenario, start_time=MIN_ST, end_time=MIN_ST + 4 * MIN_TD)
    assert elapsed is not None
    return {"keys": keys, "first_tick_ms": elapsed / 1_000_000}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keys", type=int, default=4096)
    args = parser.parse_args()
    if args.keys < 1:
        parser.error("--keys must be positive")
    logging.disable(logging.CRITICAL)
    print(json.dumps(run(args.keys)))
