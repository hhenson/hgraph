from hgraph import graph, exception_time_series, run_graph
from hgraph.nodes import const, debug_print


@graph
def capture_an_exception():
    a = const(1.0)
    b = const(0.0)
    c = a / b
    e = exception_time_series(c)
    debug_print("a / b", c)
    debug_print("exception", e)


# run_graph(capture_an_exception)


@graph
def capture_an_exception_2():
    a = const(1.0) + const(2.0)
    b = const(0.0)
    c = a / b
    e = exception_time_series(c, trace_back_depth=2, capture_values=True)
    debug_print("(1.0 + 2.0) / 0.0", c)
    debug_print("exception", e)

run_graph(capture_an_exception_2)
