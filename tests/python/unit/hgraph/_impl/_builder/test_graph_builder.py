from hgraph import graph, wire_graph, Edge
from hgraph.nodes import const, print_


def test_build_graph():

    @graph
    def hello_world():
        c = const("Hello World")
        print_(c)

    from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext
    with WiringNodeInstanceContext():
        g = wire_graph(hello_world)

    assert g.edges == tuple([Edge(src_node=0, dst_node=1, output_path=tuple(), input_path=(0,))])
    assert g.node_builders[0].signature.name == "const"
    assert g.node_builders[1].signature.name == "_print"
    assert len(g.node_builders) == 2
