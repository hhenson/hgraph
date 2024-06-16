from hgraph._operators._graph_operators import default, nothing, null_sink
from hgraph._types._ref_type import REF
from hgraph._types._time_series_types import OUT, TIME_SERIES_TYPE
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._wiring._decorators import generator, graph, compute_node, sink_node

__all__ = tuple()


@graph(overloads=default)
def default_impl(ts: OUT, default_value: OUT) -> OUT:
    """
    takes the imports, and converts them to references in the compute_node, this makes the solution more
    efficient.
    """
    return _default(ts, ts, default_value)


@compute_node(valid=tuple())
def _default(
        ts_ref: REF[OUT], ts: OUT, default_value: REF[OUT]
) -> REF[OUT]:
    if not ts.valid:
        # In case this has become invalid, we need to make sure we detect a tick from the real value.
        ts.make_active()
        return default_value.value
    else:
        ts.make_passive()
        return ts_ref.value


@generator(overloads=nothing)
def nothing_impl(tp: type[OUT] = AUTO_RESOLVE) -> OUT:
    """
    Produces no ticks ever

    :param tp: Used to resolve the correct type for the output, by default this is TS[SCALAR] where SCALAR is the type
               of the value.
    :return: A time series that will never tick
    """
    yield from ()


@sink_node(overloads=null_sink)
def null_sink_impl(ts: TIME_SERIES_TYPE):
    """
    A sink node that will consume the time-series and do nothing with it.
    This is useful when you want to consume a time-series but do not want to do anything with it.
    """