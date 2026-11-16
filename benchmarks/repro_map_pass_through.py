"""Compare shared keyed lookups with directly multiplexed inputs.

The reported reproducer times wiring, initialization, updates and teardown
together. Report that measurement separately from steady-state update time.
Use --source-size to hold the shared dictionary size constant while varying
the number of mapped children. A changed key has exactly one reader.

Example: python benchmarks/repro_map_pass_through.py --source-size 8192
"""
import argparse
import json
import logging
import statistics
import time

from hgraph import MIN_ST, MIN_TD, TS, TSD, compute_node, default, generator, graph, keys_, map_, null_sink, pass_through
from hgraph.test import eval_node


def run(n, source_size, cycles, warmup, shared_lookup):
    measurements = {"reads": 0}

    @compute_node
    def read(x: TS[int]) -> TS[int]:
        measurements["reads"] += 1
        return x.value

    @generator
    def keys() -> TSD[str, TS[int]]:
        yield MIN_ST, {f"k{i}": i for i in range(n)}

    @generator
    def shared() -> TSD[str, TS[int]]:
        yield MIN_ST, {f"k{i}": 0 for i in range(source_size)}
        for cycle in range(1, warmup + cycles + 2):
            if cycle == warmup + 1:
                measurements["started"] = time.perf_counter()
            if cycle == warmup + cycles + 1:
                measurements["steady_seconds"] = time.perf_counter() - measurements["started"]
            yield MIN_ST + MIN_TD * cycle, {f"k{cycle % n}": cycle}

    @graph
    def app():
        if shared_lookup:
            out = map_(lambda key, x, s: read(default(s[key], 0)), keys(), pass_through(shared()))
        else:
            # Explicit keys keep the control's child count equal to n even
            # when the source dictionary contains additional unused keys.
            selected_keys = keys()
            out = map_(lambda key, x, s: read(s), selected_keys, shared(), __keys__=keys_(selected_keys))
        null_sink(out)

    started = time.perf_counter()
    eval_node(app, __elide__=True)
    elapsed = time.perf_counter() - started
    assert measurements["reads"] == n + warmup + cycles + 1, measurements
    return {
        "whole_run_us_per_update": elapsed / (warmup + cycles + 1) * 1e6,
        "steady_us_per_update": measurements["steady_seconds"] / cycles * 1e6,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", type=int, default=[250, 500, 1000, 2000, 4000])
    parser.add_argument("--cycles", type=int, default=20000)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--source-size", type=int)
    args = parser.parse_args()
    if min(*args.sizes, args.cycles, args.repeats) <= 0 or args.warmup < 0:
        parser.error("sizes, cycles and repeats must be positive; warmup must be nonnegative")
    if args.source_size is not None and args.source_size < max(args.sizes):
        parser.error("source-size must cover every mapped key")
    logging.disable(logging.WARNING)
    for n in args.sizes:
        for shared_lookup in (True, False):
            samples = [run(n, args.source_size or n, args.cycles, args.warmup, shared_lookup)
                       for _ in range(args.repeats)]
            print(json.dumps({
                "mapped_keys": n, "source_keys": args.source_size or n,
                "variant": "shared_lookup" if shared_lookup else "per_key",
                "cycles": args.cycles, "warmup": args.warmup,
                **{key: statistics.median(sample[key] for sample in samples) for key in samples[0]},
                "samples": samples,
            }), flush=True)


if __name__ == "__main__":
    main()
