from hg import graph, run_graph, RunMode
from hg.nodes import const, write_str


def test_build_graph():

    @graph
    def hello_world():
        c = const("Hello World")
        write_str(c)

    run_graph(hello_world, run_mode=RunMode.BACK_TEST)
