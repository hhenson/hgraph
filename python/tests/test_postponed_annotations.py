"""Pin issue #83: string annotations resolve everywhere signatures resolve.

This module opts into PEP 563 postponed evaluation, so every annotation below
reaches the wiring layer as a STRING. Signature resolution must evaluate them
(``inspect.signature(..., eval_str=True)`` / ``inspect.get_annotations``)
uniformly across nodes, graphs, TSB schemas, compound scalars, generators,
and lifecycle functions.
"""

from __future__ import annotations

import hgraph as hg
from hgraph import TS, TSB, TimeSeriesSchema
from hgraph.test import eval_node


class Pair(TimeSeriesSchema):
    a: TS[int]
    b: TS[int]


class Point(hg.CompoundScalar):
    x: int = 0
    y: int = 0


def test_postponed_compute_node_and_graph():
    @hg.compute_node
    def add_one(value: TS[int]) -> TS[int]:
        return value.value + 1

    @hg.graph
    def app(value: TS[int]) -> TS[int]:
        return add_one(value)

    assert eval_node(app, [1, 2]) == [2, 3]


def test_postponed_tsb_schema():
    @hg.graph
    def bundle(a: TS[int], b: TS[int]) -> TSB[Pair]:
        return hg.combine[TSB[Pair]](a=a, b=b)

    assert eval_node(bundle, [1], [2]) == [{"a": 1, "b": 2}]


def test_postponed_compound_scalar():
    @hg.graph
    def to_point(x: TS[int], y: TS[int]) -> TS[Point]:
        return hg.combine[TS[Point]](x=x, y=y)

    out = eval_node(to_point, [1], [2])
    assert out == [Point(x=1, y=2)]


def test_postponed_generator_and_lifecycle():
    seen = []

    @hg.generator
    def gen(count: int) -> TS[int]:
        yield hg.MIN_ST, count

    @hg.compute_node
    def tracked(value: TS[int], _state: hg.STATE = None) -> TS[int]:
        _state.mark = value.value
        return value.value

    @tracked.stop
    def tracked_stop(_state: hg.STATE):
        seen.append(_state.mark)

    @hg.graph
    def app() -> TS[int]:
        return tracked(gen(5))

    assert eval_node(app) == [5]
    assert seen == [5]
