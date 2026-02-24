from datetime import datetime

from hgraph.test import eval_node
import pytest

from hgraph import (
    MIN_TD,
    SIGNAL,
    graph,
    compute_node,
    TS,
    TIME_SERIES_TYPE,
    evaluate_graph,
    GraphConfiguration,
    const,
    EvaluationMode,
    print_,
    debug_print,
    sink_node,
)


def test_hello_world():

    @graph
    def hello_world():
        c = const("Hello World")
        print_(c)

    evaluate_graph(hello_world, GraphConfiguration(run_mode=EvaluationMode.SIMULATION))


def test_compute_node():

    @compute_node
    def tick(ts: TIME_SERIES_TYPE) -> TS[bool]:
        return True

    @graph
    def hello_world():
        c = const(1)
        t = tick(c)
        debug_print("t", t)

    evaluate_graph(hello_world, GraphConfiguration(run_mode=EvaluationMode.SIMULATION))


def test_return_result():

    @graph
    def hello_world() -> TS[int]:
        return const(1)

    assert evaluate_graph(hello_world, GraphConfiguration()) == [(datetime(1970, 1, 1, 0, 0, 0, 1), 1)]


def test_run_no_cleanup():
    @sink_node
    def fail(ts: SIGNAL):
        raise RuntimeError
    
    @sink_node
    def cleanup(ts: SIGNAL):
        pass
    
    cleanup_called = False
    
    @cleanup.stop
    def cleaup_stop():
        nonlocal cleanup_called
        cleanup_called = True
        
    @graph
    def g():
        ts = const(True, delay=MIN_TD*2)
        fail(ts)
        cleanup(ts)
        
    with pytest.raises(Exception):
        evaluate_graph(g, GraphConfiguration())
        
    assert cleanup_called == True
    
    cleanup_called = False

    with pytest.raises(Exception):
        evaluate_graph(g, GraphConfiguration(cleanup_on_error=False))
        
    assert cleanup_called == False