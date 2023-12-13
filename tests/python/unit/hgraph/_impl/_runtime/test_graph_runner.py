from hgraph import graph, run_graph, compute_node, TS, TIME_SERIES_TYPE
from hgraph._runtime._evaluation_engine import EvaluationMode
from hgraph.nodes import const, write_str
from hgraph.nodes._print import debug_print


def test_hello_world():

    @graph
    def hello_world():
        c = const("Hello World")
        write_str(c)

    run_graph(hello_world, run_mode=EvaluationMode.SIMULATION)


def test_compute_node():

    @compute_node
    def tick(ts: TIME_SERIES_TYPE) -> TS[bool]:
        return True

    @graph
    def hello_world():
        c = const(1)
        t = tick(c)
        debug_print("t", t)

    run_graph(hello_world, run_mode=EvaluationMode.SIMULATION)