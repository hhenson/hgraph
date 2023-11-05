import typing
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, Optional

from hg._impl._types._input import PythonTimeSeriesInput, PythonBoundTimeSeriesInput
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

    @property
    def value(self) -> SCALAR:
        return self._value

    @property
    def delta_value(self) -> Optional[DELTA_SCALAR]:
        return self._value

    @value.setter
    def value(self, v: SCALAR):
        if v is None:
            return
        if not isinstance(v, self._tp):
            raise TypeError(f"Expected {self._tp}, got {type(v)}")
        self._value = v
        self.mark_modified()

    def apply_result(self, value: SCALAR):
        self.value = value

    def mark_invalid(self):
        self._value = None
        super().mark_invalid()

    def copy_from_output(self, output: "TimeSeriesOutput"):
        assert isinstance(output, PythonTimeSeriesValueOutput)
        self.value = output._value

    def copy_from_input(self, input: "TimeSeriesInput"):
        assert isinstance(input, PythonTimeSeriesValueInput)
        self.value = input.value


@dataclass
class PythonTimeSeriesValueInput(PythonBoundTimeSeriesInput, TimeSeriesValueInput[SCALAR], Generic[SCALAR]):
    """
    The only difference between a PythonBoundTimeSeriesInput and a PythonTimeSeriesValueInput is that the
    signature of value etc.
    """
