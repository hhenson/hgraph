from collections import deque
from dataclasses import dataclass, field
from typing import Generic

from hgraph._types import CompoundScalar, TimeSeriesSchema, KEYABLE_SCALAR, TIME_SERIES_TYPE, TS

__all__ = ("_BufferState", "KeyValue")


@dataclass
class _BufferState(CompoundScalar):
    buffer: deque = field(default_factory=deque)


class KeyValue(TimeSeriesSchema, Generic[KEYABLE_SCALAR, TIME_SERIES_TYPE]):
    key: TS[KEYABLE_SCALAR]
    value: TIME_SERIES_TYPE
