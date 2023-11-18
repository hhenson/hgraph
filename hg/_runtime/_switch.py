from typing import Callable, TypeVar, Optional

from hg._types._time_series_types import TIME_SERIES_TYPE
from hg._types._scalar_types import SCALAR
from hg._types._ts_type import TS

__all__ = ("ts_switch",)


def ts_switch(switches: dict[SCALAR, Callable[[...], Optional[TIME_SERIES_TYPE]]], key: TS[SCALAR], *args,
              reload_on_ticked: bool = False, **kwargs) -> Optional[TIME_SERIES_TYPE]:
    """
    The ability to select and instantiate a graph based on the value of the key time-series input.
    By default, when the key changes, a new instance of graph is created and run.
    The graph will be evaluated when it is initially created and then as the values are ticked as per normal.
    If the code depends on inputs to have ticked, they will only be evaluated when the inputs next tick (unless
    they have ticked when the graph is wired in).

    The selector is part of the graph shaping operators. This allows for different shapes that operate on the same
    inputs nad return the same output. An example of using this is when you have different order types, and then you
    dynamically choose which graph to evaluate based on the order type.

    This node has the overhead of instantiating and tearing down the sub-graphs as the key changes. The use of switch
    should consider this overhead, the positive side is that once the graph is instantiated the performance is the same
    as if it were wired in directly. This is great when the key changes infrequently.

    The mapped graphs / nodes can have a first argument which is of the same type as the key. In this case the key
    will be mapped into this argument. If the first argument is not of the same type as the key, or the kwargs match
    the argument name of the first argument, the key will be not be mapped into the argument.

    Example:
        key: TS[str] = ...
        ts1: TS[int] = ...
        ts2: TS[int] = ...
        out = switch({'add': add_, "sub": sub_}, key, ts1, ts2)

    Which will perform the computation based on the key's value.
    """
