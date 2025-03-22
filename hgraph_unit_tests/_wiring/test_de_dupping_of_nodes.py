from hgraph import graph, GraphBuilder, wire_graph, debug_print
from hgraph import const


def test_node_de_duping():
    @graph
    def main():
        """The main graph."""
        c = const(1)
        d = const(1)
        debug_print("c+d", c + d)

    from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext

    with WiringNodeInstanceContext():
        out = wire_graph(main)
    assert isinstance(out, GraphBuilder)
    assert len(out.node_builders) == 3


def test_node_de_duping_2():
    @graph
    def main():
        """The main graph."""
        c = const(1)
        debug_print("c+d", c + 1)

    from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext

    with WiringNodeInstanceContext():
        out = wire_graph(main)
    assert isinstance(out, GraphBuilder)
    assert len(out.node_builders) == 3
