from hg import graph, run_graph, RunMode, compute_node, TS, TIME_SERIES_TYPE
from hg.nodes import const, write_str
from hg.nodes._print import debug_print


def test_hello_world():

    @graph
    def hello_world():
        c = const("Hello World")
        write_str(c)

    run_graph(hello_world, run_mode=RunMode.BACK_TEST)


def test_compute_node():

    @compute_node
    def tick(ts: TIME_SERIES_TYPE) -> TS[bool]:
        return True

    @graph
    def hello_world():
        c = const(1)
        t = tick(c)
        debug_print("t", t)

    run_graph(hello_world, run_mode=RunMode.BACK_TEST)