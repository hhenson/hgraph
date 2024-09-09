from hgraph import graph, generator, compute_node, sink_node, run_graph, EvaluationMode, TS, MIN_TD, MIN_ST, TS_OUT


@generator
def counter(max_count: int) -> TS[int]:
    """A generator node that counts from 0 to max_count and then stops."""
    for i in range(max_count):
        yield MIN_ST + i * MIN_TD, i


@compute_node
def sum_time_series(ts: TS[int], _output: TS_OUT[int] = None) -> TS[int]:
    """
    A compute node that sums a time series.
    Note the use of ``_output``. This is a special named input that will be populated with the output time-series
    at runtime. It is important to name the input correctly, otherwise bad things will happen.
    """
    return _output.value + ts.value if _output.valid else ts.value


@sink_node
def print_time_series(ts: TS[int]):
    """A sink node that prints the time series."""
    print(ts.value)


@graph
def main():
    """The main graph."""
    c = counter(10)
    s = sum_time_series(c)
    print_time_series(s)


run_graph(main, run_mode=EvaluationMode.SIMULATION)
