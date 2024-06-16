from datetime import timedelta

from hgraph._wiring._decorators import generator
from hgraph._types._scalar_types import SCALAR
from hgraph._types._ts_type import TS
from hgraph._types._time_series_types import OUT
from hgraph._runtime._evaluation_engine import EvaluationEngineApi
from hgraph._operators._time_series_conversion import const

__all__ = []


@generator(overloads=const)
def const_default(
    value: SCALAR,
    tp: type[OUT] = TS[SCALAR],
    delay: timedelta = timedelta(),
    _api: EvaluationEngineApi = None,
) -> OUT:
    """
    Produces a single tick at the start of the graph evaluation after which this node does nothing.

    :param value: The value in appropriate form to be applied to the time-series type specified in tp.
    :param tp: Used to resolve the correct type for the output, by default this is TS[SCALAR] where SCALAR is the type
               of the value.
    :param delay: The amount of time to delay the value by. The default is 0.
    :param _api: The evaluation api (to get start time). (To be injected)
    :return: A single tick of the value supplied.
    """
    yield _api.start_time + delay, value
