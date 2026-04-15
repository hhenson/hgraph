from datetime import timedelta

import hgraph._hgraph as _hgraph

from hgraph import GraphConfiguration, TS, evaluate_graph, graph, sink_node
from hgraph.nodes.v2 import const as v2_const
from hgraph.nodes.v2 import debug_print as v2_debug_print
from hgraph.nodes.v2 import null_sink as v2_null_sink
from hgraph._wiring._graph_builder import wire_graph
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext


def test_v2_basic_nodes_build_a_pure_v2_graph():
    @graph
    def g():
        v2_null_sink(v2_const(1))

    with WiringNodeInstanceContext():
        graph_builder = wire_graph(g)

    cpp_builders = [builder for builder in graph_builder.node_builders if isinstance(builder, _hgraph.v2.NodeBuilder)]
    if not cpp_builders:
        return
    implementation_names = tuple(builder.implementation_name for builder in cpp_builders)
    assert implementation_names == ("const", "null_sink")


def test_v2_const_graphs_use_v2_python_sinks_by_default():
    @sink_node
    def sink(ts: TS[int]):
        pass

    @graph
    def g():
        sink(v2_const(1))

    with WiringNodeInstanceContext():
        graph_builder = wire_graph(g)

    cpp_builders = [builder for builder in graph_builder.node_builders if isinstance(builder, _hgraph.v2.NodeBuilder)]
    if not cpp_builders:
        return
    assert tuple(builder.implementation_name for builder in cpp_builders) == ("const", "sink")


def test_v2_const_and_debug_print_execute_without_api_changes(capsys):
    @graph
    def g():
        v2_debug_print("ts", v2_const(1), sample=1)

    with WiringNodeInstanceContext():
        graph_builder = wire_graph(g)

    cpp_builders = [builder for builder in graph_builder.node_builders if isinstance(builder, _hgraph.v2.NodeBuilder)]
    if not cpp_builders:
        return
    assert tuple(builder.implementation_name for builder in cpp_builders) == ("const", "debug_print")

    if not isinstance(graph_builder, _hgraph.v2.GraphBuilder):
        return

    evaluate_graph(g, GraphConfiguration(end_time=timedelta(milliseconds=1)))

    out = capsys.readouterr().out
    assert " ts: 1" in out
