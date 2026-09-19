"""A component goes wherever a graph does as a higher-order function.

``dmap_(pricing, ...)`` with ``pricing`` a component is where a recoverable
component is expected to live (RFC 0039), so a component has to be accepted as
the mapped function -- by ``map_`` and the rest as much as by ``dmap_``.
"""

import hgraph as hg
import pytest


@hg.compute_node
def doubled(ts: hg.TS[int]) -> hg.TS[int]:
    return ts.value * 2


@hg.component
def doubling(ts: hg.TS[int]) -> hg.TS[int]:
    return doubled(ts)


SCHEMA = hg.TSD[int, hg.TS[int]]
EVENTS = [{1: 1, 2: 2}, {1: 3}, {2: hg.REMOVE}, {3: 4}]
EXPECTED = [{1: 2, 2: 4}, {1: 6}, {2: hg.REMOVE}, {3: 8}]


def test_a_component_is_accepted_as_the_mapped_function():
    @hg.graph
    def app(values: SCHEMA) -> SCHEMA:
        return hg.map_(doubling, values)

    assert hg.eval_node(app, EVENTS) == EXPECTED


@pytest.mark.parametrize("in_process", [True, False], ids=["in-process", "processes"])
def test_a_component_is_accepted_as_the_distributed_mapped_function(in_process):
    @hg.graph
    def app(values: SCHEMA) -> SCHEMA:
        return hg.dmap_(doubling, values, __workers__=2, in_process=in_process)

    assert hg.eval_node(app, EVENTS) == EXPECTED


def test_wrapping_the_mapped_function_in_a_component_changes_nothing():
    @hg.graph
    def plain(values: SCHEMA) -> SCHEMA:
        return hg.dmap_(doubled, values, __workers__=2, in_process=True)

    @hg.graph
    def wrapped(values: SCHEMA) -> SCHEMA:
        return hg.dmap_(doubling, values, __workers__=2, in_process=True)

    assert hg.eval_node(wrapped, EVENTS) == hg.eval_node(plain, EVENTS)
