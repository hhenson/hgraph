from typing import Callable, TypeVar

from hg._types._scalar_types import SCALAR
from hg._types._ts_type import TS

__all__ = ("switch",)

SWITCH_SIGNATURE = TypeVar("SWITCH_SIGNATURE", bound=Callable)


def switch(switches: dict[SCALAR, SWITCH_SIGNATURE], ts: TS[SCALAR],
           reload_on_ticked: bool = False) -> SWITCH_SIGNATURE:
    """
    A switch statement that will select and instantiate a graph instance based on the value of the switch time-series.
    The return value is a node of the same input and output signature as teh switch statement.

    Each time the ts actually changes value, a new instance of the component associated with the value will be
    instantiated. If the ts ticks, but the value is not changed, then the same instance will be used unless
    reload_on_ticked is set to True, in that case the component will be re-instantiated each time the value ticks.

    Note: The contributed components can only take time-series values, any scalar values will need to be
          wrapped inside a graph.

    Usage:

        @graph
        def graph1(ts1, ..., tsn):
            ...

        @graph
        def graph2(ts1, ..., tsn):
            ...

        ts: TS[int] = ...

        switch( {1: graph1, 2: graph2}, ts)(ts1, ..., tsn)
    """
