import typing
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, Optional

from hg._impl._types._input import PythonTimeSeriesInput
from hg._impl._types._output import PythonTimeSeriesOutput
from hg._impl._types._scalar_value import PythonScalarValue
from hg._runtime._constants import MIN_DT
from hg._types._scalar_types import SCALAR
from hg._types._scalar_value import ScalarValue
from hg._types._time_series_types import DELTA_SCALAR
from hg._types._ts_type import TimeSeriesValueOutput, TimeSeriesValueInput


__all__ = ("PythonTimeSeriesValueOutput", "PythonTimeSeriesValueInput")

if typing.TYPE_CHECKING:
    from hg._types._time_series_types import TimeSeriesOutput, TimeSeriesInput


@dataclass
class PythonTimeSeriesValueOutput(PythonTimeSeriesOutput, TimeSeriesValueOutput[SCALAR], Generic[SCALAR]):

    _tp: type = None
    _value: SCALAR = None
    _last_modified_time: datetime = MIN_DT

    @property
    def scalar_value(self) -> Optional[ScalarValue]:
        if self._value is None:
            return None
        return PythonScalarValue(self._tp, self._value)

    @scalar_value.setter
    def scalar_value(self, value: ScalarValue):
        self._value = value.cast(self._tp)

    @property
    def delta_scalar_value(self) -> Optional[ScalarValue]:
        if self._value is None:
            return None
        return PythonScalarValue(self._tp, self._value)

    @property
    def value(self) -> SCALAR:
        return self._value

    @property
    def delta_value(self) -> Optional[DELTA_SCALAR]:
        return self._value

    @value.setter
    def value(self, v: SCALAR):
        if not isinstance(v, self._tp):
            raise TypeError(f"Expected {self._tp}, got {type(v)}")
        self._value = v
        context = self.owning_graph.context
        et = context.current_engine_time
        if self._last_modified_time < et:
            self._last_modified_time = et
            self._notify()

    def apply_result(self, value: SCALAR):
        self.value = value

    @property
    def modified(self) -> bool:
        context = self.owning_graph.context
        return context.current_engine_time == self._last_modified_time

    @property
    def valid(self) -> bool:
        return self.owning_graph.context.current_engine_time > MIN_DT

    @property
    def all_valid(self) -> bool:
        return self.valid

    @property
    def last_modified_time(self) -> datetime:
        return self._last_modified_time

    def mark_invalid(self):
        self._value = None
        self._last_modified_time = MIN_DT
        self._notify()

    def copy_from_output(self, output: "TimeSeriesOutput"):
        assert isinstance(output, PythonTimeSeriesValueOutput)
        self.value = output._value

    def copy_from_input(self, input: "TimeSeriesInput"):
        assert isinstance(input, PythonTimeSeriesValueInput)
        self.value = input.value


@dataclass
class PythonTimeSeriesValueInput(PythonTimeSeriesInput, TimeSeriesValueInput[SCALAR], Generic[SCALAR]):

    _output: PythonTimeSeriesValueOutput = None
    _active: bool = False

    @property
    def output(self) -> TimeSeriesValueOutput[SCALAR]:
        return self._output

    @property
    def bound(self) -> bool:
        return True

    @property
    def active(self) -> bool:
        return self._active

    def make_active(self):
        if not self._active:
            self._active = True
            self._output.subscribe_node(self.owning_node)

    def make_passive(self):
        if self._active:
            self._active = False
            self._output.un_subscribe_node(self.owning_node)

    @property
    def value(self) -> Optional[SCALAR]:
        return self._output.value

    @property
    def delta_value(self) -> Optional[DELTA_SCALAR]:
        return self._output.delta_value

    @property
    def scalar_value(self) -> ScalarValue:
        return self._output.scalar_value

    @property
    def delta_scalar_value(self) -> ScalarValue:
        return self._output.delta_scalar_value

    @property
    def modified(self) -> bool:
        return self._output.modified

    @property
    def valid(self) -> bool:
        return self._output.valid

    @property
    def all_valid(self) -> bool:
        return self._output.all_valid

    @property
    def last_modified_time(self) -> datetime:
        return self._output.last_modified_time