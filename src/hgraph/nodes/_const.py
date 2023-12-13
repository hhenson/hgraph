from datetime import timedelta
from typing import Type

from hgraph import generator, SCALAR, TIME_SERIES_TYPE, EvaluationClock, TS, compute_node, graph, REF

__all__ = ("const", "default")


@generator
def const(value: SCALAR, tp: Type[TIME_SERIES_TYPE] = TS[SCALAR], delay: timedelta = timedelta(),
          context: EvaluationClock = None) -> TIME_SERIES_TYPE:
    """
    Produces a single tick at the start of the graph evaluation after which this node does nothing.

    :param value: The value in appropriate form to be applied to the time-series type specified in tp.
    :param tp: Used to resolve the correct type for the output, by default this is TS[SCALAR] where SCALAR is the type
               of the value.
    :param delay: The amount of time to delay the value by. The default is 0.
    :param context: The execution context.
    :return: A single tick of the value supplied.
    """
    yield context.evaluation_time + delay, value


@graph
def default(ts: TIME_SERIES_TYPE, default_value: SCALAR) -> TIME_SERIES_TYPE:
    """
    Returns the time-series ts with any missing values replaced with default_value.

    :param ts: The time-series to replace missing values in.
    :param default_value: The value to replace missing values with.
    :return: The time-series with missing values replaced with default_value.
    """
    c = const(default_value, tp=ts.output_type.py_type)
    return _default(ts, ts, c)


@compute_node(valid=tuple())
def _default(ts_ref: REF[TIME_SERIES_TYPE], ts: TIME_SERIES_TYPE, default_value: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
    if not ts.valid:
        # In case this has become invalid, we need to make sure we detect a tick from the real value.
        ts.make_active()
        return default_value.value
    else:
        ts.make_passive()
        return ts_ref.value
