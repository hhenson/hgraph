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


@hg.compute_node
def scaled(ts: hg.TS[int], scale: int) -> hg.TS[int]:
    return ts.value * scale


@hg.graph
def scaling_graph(ts: hg.TS[int], scale: int) -> hg.TS[int]:
    return scaled(ts, scale)


@hg.component
def scaling(ts: hg.TS[int], scale: int) -> hg.TS[int]:
    return scaled(ts, scale)


SCALED = [{1: 3, 2: 6}, {1: 9}, {2: hg.REMOVE}, {3: 12}]


def test_a_components_scalar_parameters_are_bound_as_a_graphs_are():
    # Found by adversarial review: a scalar was handed to C++ as one more
    # argument, and ``map_`` matched no overload -- for the component only.
    @hg.graph
    def as_graph(values: SCHEMA) -> SCHEMA:
        return hg.map_(scaling_graph, values, scale=3)

    @hg.graph
    def as_component(values: SCHEMA) -> SCHEMA:
        return hg.map_(scaling, values, scale=3)

    assert hg.eval_node(as_graph, EVENTS) == SCALED
    assert hg.eval_node(as_component, EVENTS) == SCALED


@pytest.mark.parametrize("in_process", [True, False], ids=["in-process", "processes"])
def test_a_components_scalar_parameters_reach_distributed_workers(in_process):
    @hg.graph
    def app(values: SCHEMA) -> SCHEMA:
        return hg.dmap_(scaling, values, scale=3, __workers__=2, in_process=in_process)

    assert hg.eval_node(app, EVENTS) == SCALED


def test_a_component_maps_over_a_fixed_size_list():
    # A fixed-size list is unrolled inline, once per index on one wiring; the
    # second index used to be refused as a duplicate recordable id.
    @hg.graph
    def app(values: hg.TSL[hg.TS[int], hg.Size[3]]) -> hg.TSL[hg.TS[int], hg.Size[3]]:
        return hg.map_(doubling, values)

    assert hg.eval_node(app, [(1, 2, 3), {1: 5}]) == [{0: 2, 1: 4, 2: 6}, {1: 10}]
