from hgraph import (
    graph,
    exception_time_series,
    TS,
    try_except,
    const,
    debug_print,
    GraphConfiguration,
    evaluate_graph,
)


@graph
def capture_an_exception():
    a = const(1.0)
    b = const(0.0)
    c = a / b
    e = exception_time_series(c)
    debug_print("a / b", c)
    debug_print("exception", e)


evaluate_graph(capture_an_exception, GraphConfiguration())


@graph
def capture_an_exception_2():
    a = const(1.0) + const(2.0)
    b = const(0.0)
    c = a / b
    e = exception_time_series(c, trace_back_depth=2, capture_values=True)
    debug_print("(1.0 + 2.0) / 0.0", c)
    debug_print("exception", e)


evaluate_graph(capture_an_exception_2, GraphConfiguration())


@graph
def a_graph(lhs: TS[float], rhs: TS[float]) -> TS[float]:
    return lhs / rhs


@graph
def capture_an_exception_3():
    result = try_except(a_graph, const(1.0), const(0.0))
    debug_print("(1.0 + 2.0) / 0.0", result.out)
    debug_print("exception", result.exception)


evaluate_graph(capture_an_exception_3, GraphConfiguration())
