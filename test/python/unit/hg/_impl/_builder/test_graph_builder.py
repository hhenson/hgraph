from hg import graph, wire_graph, Edge
from hg.nodes._const import const
from hg.nodes._write import write_str


def test_build_graph():

    @graph
    def hello_world():
        c = const("Hello World")
        write_str(c)

    g = wire_graph(hello_world)

    assert g.edges == tuple([Edge(src_node=0, dst_node=1, output_path=(0,), input_path=(0,))])
    assert g.node_builders[0].signature.name == "const"
    assert g.node_builders[1].signature.name == "write_str"
    assert len(g.node_builders) == 2
