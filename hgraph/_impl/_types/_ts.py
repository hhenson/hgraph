import typing
from dataclasses import dataclass
from typing import Generic, Optional

from hgraph._impl._impl_configuration import HG_TYPE_CHECKING
from hgraph._impl._types._input import PythonBoundTimeSeriesInput
from hgraph._impl._types._output import PythonTimeSeriesOutput
from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_types import DELTA_SCALAR
from hgraph._types._ts_type import TimeSeriesValueOutput, TimeSeriesValueInput

__all__ = ("PythonTimeSeriesValueOutput", "PythonTimeSeriesValueInput")

if typing.TYPE_CHECKING:
    from hgraph._types._time_series_types import TimeSeriesOutput, TimeSeriesInput


@dataclass
class PythonTimeSeriesValueOutput(PythonTimeSeriesOutput, TimeSeriesValueOutput[SCALAR], Generic[SCALAR]):

    _tp: type | None = None
    _value: Optional[SCALAR] = None

    @property
    def value(self) -> SCALAR:
        return self._value

    @property
    def delta_value(self) -> Optional[DELTA_SCALAR]:
        return self._value

    @value.setter
    def value(self, v: SCALAR):
        if v is None:
            self.invalidate()
            return

        if HG_TYPE_CHECKING:
            tp_ = origin if (origin := typing.get_origin(self._tp)) else self._tp
            if not isinstance(v, tp_):
                raise TypeError(f"Expected {self._tp}, got {type(v)}")

        self._value = v
        self.mark_modified()

    def clear(self):
        pass

    def invalidate(self):
        self.mark_invalid()

    def can_apply_result(self, result: SCALAR) -> bool:
        return not self.modified

    def apply_result(self, result: SCALAR):
        if result is None:
            return
        try:
            self.value = result
        except Exception as e:
            raise TypeError(
                f"Cannot apply node output {result} of type {result.__class__.__name__} to {self}: {e}"
            ) from e

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
