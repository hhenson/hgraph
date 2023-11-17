from typing import Type

from hg import generator, SCALAR, TIME_SERIES_TYPE, ExecutionContext, TS, MIN_DT

__all__ = ("const",)


@generator
def const(value: SCALAR, tp: Type[TIME_SERIES_TYPE] = TS[SCALAR], context: ExecutionContext = None) -> TIME_SERIES_TYPE:
    """
    Produces a single tick at the start of the graph evaluation after which this node does nothing.

    :param value: The value in appropriate form to be applied to the time-series type specified in tp.
    :param tp: Used to resolve the correct type for the output, by default this is TS[SCALAR] where SCALAR is the type
               of the value.
    :param context: The execution context.
    :return: A single tick of the value supplied.
    """
    yield context.current_engine_time, value

