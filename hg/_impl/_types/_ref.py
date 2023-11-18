import typing
from dataclasses import dataclass
from typing import Generic, TYPE_CHECKING

from hg._types._time_series_types import TimeSeriesInput, TIME_SERIES_TYPE, TimeSeriesOutput
from hg._impl._types._input import PythonBoundTimeSeriesInput
from hg._impl._types._output import PythonTimeSeriesOutput
from hg._types._ref_type import TimeSeriesReference, TimeSeriesReferenceOutput, TimeSeriesReferenceInput
from hg._types._scalar_types import SCALAR

if TYPE_CHECKING:
    from hg._builder._input_builder import InputBuilder


__all__ = ("PythonTimeSeriesReference", "PythonTimeSeriesReferenceOutput", "PythonTimeSeriesReferenceInput")


class PythonTimeSeriesReference(TimeSeriesReference):
    def __init__(self, ts_input: TimeSeriesInput):
        if isinstance(ts_input, TimeSeriesReferenceInput):
            ref = ts_input.value
            if has_peer := ref.has_peer:
                self.output = ref.output
            else:
                self.items = ref.items
            self.has_peer = ref.has_peer
            tp = ref.tp
        elif has_peer := ts_input.has_peer:
            self.output = ts_input.output
            tp = type(ts_input)
        else:
            # Rely on the assumption that all time-series' that support peering are also iterable.
            self.items = [PythonTimeSeriesReference(item) for item in ts_input]
            tp = type(ts_input)

        self.has_peer = has_peer
        self.tp = tp

    def bind_input(self, ts_input: TimeSeriesInput):
        # TODO: How is this different to the constructor, other than when called?
        if not isinstance(ts_input, self.tp):
            raise TypeError(f"Cannot bind reference of type {self.tp} to {type(ts_input)}")

        if self.has_peer:
            ts_input.bind_output(self.output)
        else:
            for item, r in zip((ts_input, self.items)):
                r.bind_input(item)


@dataclass
class PythonTimeSeriesReferenceOutput(PythonTimeSeriesOutput, TimeSeriesReferenceOutput, Generic[TIME_SERIES_TYPE]):

    _tp: type = None
    _value: typing.Optional[TimeSeriesReference] = None

    @property
    def value(self) -> TimeSeriesReference:
        return self._value

    @property
    def delta_value(self) -> TimeSeriesReference:
        return self._value

    @value.setter
    def value(self, v: TimeSeriesReference):
        # This should not be called with None, use mark_invalid instead
        # None will cause the type check failure below.
        if not isinstance(v, TimeSeriesReference):
            raise TypeError(f"Expected TimeSeriesReference, got {type(v)}")
        self._value = v
        self.mark_modified()

    def apply_result(self, result: SCALAR):
        if result is None:
            return
        self.value = result

    def mark_invalid(self):
        self._value = None
        super().mark_invalid()

    def copy_from_output(self, ts_output: TimeSeriesOutput):
        assert isinstance(ts_output, PythonTimeSeriesReferenceOutput)
        self.value = ts_output._value

    def copy_from_input(self, ts_input: TimeSeriesInput):
        assert isinstance(ts_input, PythonTimeSeriesReferenceInput)
        self.value = ts_input.value


@dataclass
class PythonTimeSeriesReferenceInput(PythonBoundTimeSeriesInput, TimeSeriesReferenceInput, Generic[TIME_SERIES_TYPE]):
    """
    Reference input behaves just like a scalar input if bound to a reference output. If bound to a non-reference
    time series it will create a reference to it and show it as its value
    """

    _input_impl_builder: "InputBuilder" = None
    _input_impl: typing.Optional[TimeSeriesInput] = None
    _value: typing.Optional[TimeSeriesReference] = None

    def __post_init__(self):
        self._input_impl = self._input_impl_builder.make_instance(self)

    def bind_output(self, output: TimeSeriesOutput):
        if isinstance(output, TimeSeriesReferenceOutput):
            super().bind_output(output)
        else:
            self._input_impl.bind_output(output)
            self._value = PythonTimeSeriesReference(self._input_impl)

    def value(self):
        if self._value:
            return self._value
        else:
            return super().value()

    def delta_value(self):
        if self._value:
            return self._value
        else:
            return super().delta_value()

