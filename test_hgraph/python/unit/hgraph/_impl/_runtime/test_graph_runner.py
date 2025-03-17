from datetime import datetime

from hgraph import graph, run_graph, compute_node, TS, TIME_SERIES_TYPE, evaluate_graph, GraphConfiguration, const, EvaluationMode, print_, debug_print


def test_hello_world():

    @graph
    def hello_world():
        c = const("Hello World")
        print_(c)

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


def test_return_result():

    @graph
    def hello_world() -> TS[int]:
        return const(1)

    assert evaluate_graph(hello_world, GraphConfiguration()) == [(datetime(1970, 1, 1, 0, 0, 0, 1), 1)]
