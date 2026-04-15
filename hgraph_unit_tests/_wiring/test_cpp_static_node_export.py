from contextlib import contextmanager
from datetime import timedelta

import hgraph._hgraph as _hgraph
import pytest

from hgraph import TS, TSD, GraphConfiguration, const, evaluate_graph, generator, graph, sink_node
from hgraph._builder._graph_builder import GraphBuilderFactory
from hgraph._wiring._graph_builder import wire_graph
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext
from hgraph._wiring._wiring_node_class import BaseWiringNodeClass


@contextmanager
def use_python_graph_builder():
    declared_factory = GraphBuilderFactory.declared() if GraphBuilderFactory.is_declared() else None
    GraphBuilderFactory.un_declare()
    try:
        yield
    finally:
        if declared_factory is not None:
            GraphBuilderFactory.declare(declared_factory)


def test_cpp_static_compute_node_exports_wiring_signature():
    static_sum = _hgraph.v2.static_sum

    assert isinstance(static_sum, BaseWiringNodeClass)
    assert static_sum.signature.signature == "static_sum(lhs: TS[int], rhs: TS[int]) -> TS[int]"
    assert static_sum.signature.active_inputs == frozenset(("lhs", "rhs"))
    assert static_sum.signature.valid_inputs == frozenset(("lhs", "rhs"))


def test_cpp_static_compute_node_exports_selector_policy_metadata():
    static_policy = _hgraph.v2.static_policy

    assert isinstance(static_policy, BaseWiringNodeClass)
    assert static_policy.signature.signature == "static_policy(lhs: TS[int], rhs: TS[int], strict: TS[int]) -> TS[int]"
    assert static_policy.signature.active_inputs == frozenset(("lhs", "strict"))
    assert static_policy.signature.valid_inputs == frozenset(("lhs",))
    assert static_policy.signature.all_valid_inputs == frozenset(("strict",))


def test_cpp_static_compute_node_wires_into_python_graph_builder():
    static_sum = _hgraph.v2.static_sum

    @generator
    def src(value: int) -> TS[int]:
        yield value

    @sink_node
    def sink(ts: TS[int]):
        pass

    @graph
    def g():
        sink(static_sum(src(1), src(2)))

    with use_python_graph_builder(), WiringNodeInstanceContext():
        graph_builder = wire_graph(g)

    cpp_builders = [builder for builder in graph_builder.node_builders if isinstance(builder, _hgraph.v2.NodeBuilder)]
    cpp_builder = next(builder for builder in cpp_builders if builder.implementation_name == "static_sum")
    cpp_node_index = next(index for index, builder in enumerate(graph_builder.node_builders) if builder is cpp_builder)

    assert cpp_builder.signature.signature == "static_sum(lhs: TS[int], rhs: TS[int]) -> TS[int]"
    assert cpp_builder.implementation_name == "static_sum"
    assert cpp_builder.input_schema is not None
    assert cpp_builder.output_schema is not None

    input_edges = {tuple(edge.input_path) for edge in graph_builder.edges if edge.dst_node == cpp_node_index}
    output_edges = [edge for edge in graph_builder.edges if edge.src_node == cpp_node_index]

    assert input_edges == {(0,), (1,)}
    assert len(output_edges) == 1


def test_cpp_static_generic_compute_node_exports_linked_type_vars_and_resolves_on_wiring():
    static_get_item = _hgraph.v2.static_get_item

    assert isinstance(static_get_item, BaseWiringNodeClass)
    assert static_get_item.signature.signature == "static_get_item(ts: TSD[K, V], key: TS[K]) -> V"
    assert static_get_item.signature.unresolved_args == frozenset(("ts", "key"))

    @sink_node
    def sink_str(ts: TS[str]):
        pass

    @graph
    def g():
        sink_str(static_get_item(const({1: "one"}, TSD[int, TS[str]]), const(1)))

    with use_python_graph_builder(), WiringNodeInstanceContext():
        graph_builder = wire_graph(g)

    cpp_builders = [builder for builder in graph_builder.node_builders if isinstance(builder, _hgraph.v2.NodeBuilder)]
    cpp_builder = next(builder for builder in cpp_builders if builder.implementation_name == "static_get_item")
    assert cpp_builder.signature.signature == "static_get_item(ts: TSD[int, TS[str]], key: TS[int]) -> TS[str]"
    assert cpp_builder.input_schema is not None
    assert cpp_builder.output_schema is not None


def test_cpp_static_compute_node_exports_state_injectable_metadata():
    static_typed_state = _hgraph.v2.static_typed_state

    assert isinstance(static_typed_state, BaseWiringNodeClass)
    assert static_typed_state.signature.args == ("lhs", "_state")
    assert static_typed_state.signature.uses_state
    assert not static_typed_state.signature.uses_recordable_state


def test_cpp_static_compute_node_exports_recordable_state_builder():
    static_recordable_state = _hgraph.v2.static_recordable_state

    assert isinstance(static_recordable_state, BaseWiringNodeClass)
    assert static_recordable_state.signature.args == ("lhs", "_recordable_state")
    assert static_recordable_state.signature.uses_recordable_state

    @generator
    def src(value: int) -> TS[int]:
        yield value

    @sink_node
    def sink(ts: TS[int]):
        pass

    @graph
    def g():
        sink(static_recordable_state(src(1)))

    with use_python_graph_builder(), WiringNodeInstanceContext():
        graph_builder = wire_graph(g)

    cpp_builders = [builder for builder in graph_builder.node_builders if isinstance(builder, _hgraph.v2.NodeBuilder)]
    cpp_builder = next(builder for builder in cpp_builders if builder.implementation_name == "static_recordable_state")
    assert cpp_builder.recordable_state_builder is not None


def test_cpp_static_compute_node_exports_evaluation_clock_metadata():
    static_clock = _hgraph.v2.static_clock

    assert isinstance(static_clock, BaseWiringNodeClass)
    assert static_clock.signature.args == ("lhs", "_clock")
    assert static_clock.signature.uses_clock


def test_cpp_static_mixed_graphs_wire_through_v2_builder():
    static_sum = _hgraph.v2.static_sum

    @generator
    def src(value: int) -> TS[int]:
        yield value

    @sink_node
    def sink(ts: TS[int]):
        pass

    @graph
    def g():
        sink(static_sum(src(1), src(2)))

    with WiringNodeInstanceContext():
        graph_builder = wire_graph(g)

    cpp_builders = [builder for builder in graph_builder.node_builders if isinstance(builder, _hgraph.v2.NodeBuilder)]
    assert any(builder.implementation_name == "static_sum" for builder in cpp_builders)


def test_cpp_static_graphs_wire_with_v2_node_builders():
    static_tick = _hgraph.v2.static_tick
    static_sink = _hgraph.v2.static_sink

    @graph
    def g():
        static_sink(static_tick())

    with WiringNodeInstanceContext():
        graph_builder = wire_graph(g)
    cpp_builders = [builder for builder in graph_builder.node_builders if isinstance(builder, _hgraph.v2.NodeBuilder)]
    assert {builder.implementation_name for builder in cpp_builders} >= {"static_tick", "static_sink"}

    if not isinstance(graph_builder, _hgraph.v2.GraphBuilder):
        return

    _hgraph.v2.reset_static_sink_state()
    evaluate_graph(g, GraphConfiguration(end_time=timedelta(milliseconds=1)))

    assert _hgraph.v2.static_sink_call_count() == 1
    assert _hgraph.v2.static_sink_last_value() == 42
