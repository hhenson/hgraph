from contextlib import contextmanager
from typing import List, TYPE_CHECKING, Union

from hgraph._types._error_type import NodeError
from hgraph._types._scalar_types import SCALAR, SIZE
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._ts_type import TS
from hgraph._types._tsd_type import TSD
from hgraph._types._tsl_type import TSL

if TYPE_CHECKING:
    from hgraph._wiring._wiring import WiringPort

__all__ = ("ErrorTimeSeriesBuilder", "error_context", "error_time_series")


class ErrorTimeSeriesBuilder:
    _INSTANCE: List["ErrorTimeSeriesBuilder"] = []

    def __init__(self):
        self.nodes: List["WiringPort"] = []
        self._output = None

    def __enter__(self):
        ErrorTimeSeriesBuilder._INSTANCE.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        ErrorTimeSeriesBuilder._INSTANCE.pop()

    @property
    def output(self):
        if self._output is None:
            from hgraph import CustomMessageWiringError
            raise CustomMessageWiringError("Error output is not available inside the context of the error block")
        return self._output

    @staticmethod
    def track_node(node: "WiringPort"):
        if ErrorTimeSeriesBuilder._INSTANCE:
            ErrorTimeSeriesBuilder._INSTANCE[-1].nodes.append(node)


@contextmanager
def error_context():
    with ErrorTimeSeriesBuilder() as error_builder:
        yield error_builder
    if error_builder.nodes:
        from hgraph.nodes import merge
        from hgraph import NodeError, TS, TSL
        errors = TSL[TS[NodeError]].from_ts(*(node.__error__ for node in error_builder.nodes))
        error_builder._output = merge(errors)


def error_time_series(ts: TIME_SERIES_TYPE) -> Union[
    TSL[TS[NodeError], SIZE], TSD[SCALAR, TS[NodeError]], TS[NodeError]]:
    return ts.__error__
