from hg import graph, wire_graph
from hg.nodes._const import const
from hg.nodes._write import write_str


def test_build_graph():

    @graph
    def hello_world():
        c = const("Hello World")
        write_str(c)

    g = wire_graph(hello_world)

    assert g