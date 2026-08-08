"""Focused TSD map/reduce attribution benchmark.

Run: ``uv run python benchmarks/repro_sparse_scan.py
[passthrough,map_only,reduce_only,full] [sparse,dense]``.
"""
import sys
import time

import hgraph as hg
from hgraph import TS, TSD, graph, generator, map_, null_sink, reduce

variant = sys.argv[1]
shape = sys.argv[2] if len(sys.argv) > 2 else "sparse"
if shape not in {"sparse", "dense"}:
    raise ValueError(f"unknown TSD pulse shape: {shape}")

KEYS, CYCLES, PER = (2000, 2000, 5) if shape == "sparse" else (200, 1000, 200)

@generator
def src() -> TSD[int, TS[int]]:
    if shape == "dense":
        for i in range(CYCLES):
            yield hg.MIN_TD, {k: i + k for k in range(KEYS)}
    else:
        yield hg.MIN_TD, {k: 0 for k in range(KEYS)}
        for i in range(1, CYCLES):
            base = (i * PER) % KEYS
            yield hg.MIN_TD, {(base + j) % KEYS: i for j in range(PER)}

@graph
def mapped(v: TS[int]) -> TS[int]:
    return (v + 1) * 2

@graph
def g():
    s = src()
    if variant == "map_only":
        null_sink(map_(mapped, s))
    elif variant == "reduce_only":
        null_sink(reduce(hg.add_, s, 0))
    elif variant == "passthrough":
        null_sink(s)
    else:
        null_sink(reduce(hg.add_, map_(mapped, s), 0))

t0 = time.perf_counter()
hg.run_graph(g, start_time=hg.MIN_ST, end_time=hg.MIN_ST + (CYCLES + 2) * hg.MIN_TD)
print(shape, variant, round(time.perf_counter() - t0, 3), "s")
