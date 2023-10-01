from dataclasses import dataclass
from datetime import datetime
from typing import Generic

from hg import SCALAR
from hg._types._scalar_value import ScalarValue
from hg._types._ts_type import TimeSeriesValueOutput


@dataclass
class PythonTimeSeriesValueOutput(TimeSeriesValueOutput[SCALAR], Generic[SCALAR]):
    _tp: type
    scalar_value: ScalarValue

    @property
    def value(self) -> SCALAR:
        return self.scalar_value.cast(self._tp)

    @value.setter
    def value(self, value: SCALAR):
        self.scalar_value = value

    def apply_result(self, value: SCALAR):
        self.scalar_value = value

    def subscribe_node(self, node: "Node"):
        pass

    def un_subscribe_node(self, node: "Node"):
        pass

    @property
    def modified(self) -> bool:
        pass

    @property
    def valid(self) -> bool:
        pass

    @property
    def all_valid(self) -> bool:
        pass

    @property
    def last_modified_time(self) -> datetime:
        pass