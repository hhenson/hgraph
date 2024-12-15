import typing
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Generic, Optional

import numpy as np

from hgraph._impl._types._input import PythonBoundTimeSeriesInput
from hgraph._impl._types._output import PythonTimeSeriesOutput
from hgraph._types._scalar_types import SCALAR
from hgraph._types._scalar_value import Array
from hgraph._runtime._constants import MIN_TD, MIN_DT
from hgraph._impl._impl_configuration import HG_TYPE_CHECKING
from hgraph._types._tsw_type import TimeSeriesWindowOutput, WINDOW_SIZE, WINDOW_SIZE_MIN, TimeSeriesWindowInput

__all__ = ("PythonTimeSeriesFixedWindowOutput", "PythonTimeSeriesTimeWindowOutput",
           "PythonTimeSeriesWindowInput")


@dataclass
class PythonTimeSeriesFixedWindowOutput(
    PythonTimeSeriesOutput,
    TimeSeriesWindowOutput[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN],
    Generic[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]
):

    _tp: type | None = None
    _value: Optional[Array[SCALAR]] = None
    _times: Optional[Array[datetime]] = None
    _size: int = -1
    _min_size: int = -1
    _start: int = 0
    _length: int = 0

    def __post_init__(self):
        self._value = np.ndarray(shape=[self._size], dtype=self._tp)
        self._times = np.full(shape=[self._size], fill_value=MIN_DT, dtype=datetime)

    @property
    def value(self) -> SCALAR:
        buffer: Array[SCALAR] = self._value
        capacity: int = self._size
        start: int = self._start
        length: int = self._length
        if length < self._min_size:
            return None
        if length != capacity:
            return buffer[:length]
        else:
            return np.roll(buffer, -start)

    @value.setter
    def value(self, value: Array[SCALAR]) -> None:
        """This is only for internal use, the times must be set at the same time"""
        if value is None:
            self.invalidate()
            return
        if (l := len(value) )> self._size:
            raise ValueError("Setting value with size greater than set for this output")
        elif l == self._size:
            np.copyto(self._value, value)
        else:
            self._value[:l] = value
        self.mark_modified()

    @property
    def delta_value(self) -> Optional[SCALAR]:
        if self._length < self._min_size:
            return None
        elif self._length < self._size:
            pos = self._length - 1
        else:
            pos = (self._start + self._length - 1) % self._size
        if (tm := self._times[pos]) == self.owning_graph.evaluation_clock.evaluation_time:
            return self._value[pos]
        else:
            return None

    @property
    def value_times(self) -> Array[datetime]:
        if self._length < self._min_size:
            return None
        elif self._length < self._size:
            return self._times[:self._length]
        if self._start == 0:
            return self._times
        else:
            return np.roll(self._times, -self._start)

    @value_times.setter
    def value_times(self, value: Array[SCALAR]) -> None:
        if len(value) != self._size:
            self._times[:] = value
        else:
            np.copyto(self._times, value)

    def invalidate(self):
        self.mark_invalid()

    def can_apply_result(self, result: SCALAR) -> bool:
        return not self.modified

    def apply_result(self, result: SCALAR):
        if result is None:
            return
        try:
            if HG_TYPE_CHECKING:
                tp_ = origin if (origin := typing.get_origin(self._tp)) else self._tp
                if not isinstance(result, tp_):
                    raise TypeError(f"Expected {self._tp}, got {type(result)}")
            buffer: Array[SCALAR] = self._value
            index: Array[datetime] = self._times
            capacity: int = self._size
            start: int = self._start
            length: int = self._length
            length += 1
            if length > capacity:
                start += 1
                start %= capacity
                self._start = start
                length = capacity
            self._length = length
            pos = (start + length - 1) % capacity
            buffer[pos] = result
            index[pos] = self.owning_graph.evaluation_clock.evaluation_time
            self.mark_modified()
        except Exception as e:
            raise TypeError(f"Cannot apply node output {result} of "
                            f"type {result.__class__.__name__} to {self}: {e}") from e

    def clear(self):
        # TODO: what is the right semantics here? Value time series don't do anything, collections remove all items
        pass

    def mark_invalid(self):
        self._value = np.ndarray(shape=[self._size], dtype=self._tp)
        self._times = np.full(shape=[self._size], fill_value=MIN_TD, dtype=datetime)
        super().mark_invalid()

    def copy_from_output(self, output: "TimeSeriesOutput"):
        assert isinstance(output, PythonTimeSeriesFixedWindowOutput)
        self.value = output._value
        self.value_times = output._times

    def copy_from_input(self, input: "TimeSeriesInput"):
        assert isinstance(input, PythonTimeSeriesWindowInput)
        assert isinstance(input.output, PythonTimeSeriesFixedWindowOutput)
        self.value = input.output._value
        self.value_times = input.output._times

    @property
    def first_modified_time(self) -> datetime:
        return self._times[0] if self._times else MIN_DT

    @property
    def size(self) -> int | timedelta:
        return self._size

    @property
    def min_size(self) -> int | timedelta:
        return self._min_size

    def __len__(self) -> int:
        return self._size


@dataclass
class PythonTimeSeriesTimeWindowOutput(
    PythonTimeSeriesOutput,
    TimeSeriesWindowOutput[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN],
    Generic[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]
):
    _tp: type | None = None
    _value: deque[SCALAR] = field(default_factory=deque)
    _times: deque[timedelta] = field(default_factory=deque)
    _size: timedelta = None
    _min_size: timedelta = None
    _ready: bool = False

    def _roll(self):
        tm = self.owning_graph.evaluation_clock.evaluation_time - self._size
        if self._times and self._times[0] < tm:
            while self._times and self._times[0] < tm:
                self._times.popleft()
                self._value.popleft()

    @property
    def ready(self) -> bool:
        """
        Make sure we have been running long enough (min_size) to capture enough data to meet the minimum requirement
        of time to capture ticks. This does not ensure there are ticks within or without of this scope.
        """
        if not self._ready:
            self._ready = self.owning_graph.evaluation_clock.evaluation_time - \
                          self.owning_graph.evaluation_engine_api.start_time >= self._min_size
        return self._ready

    @property
    def value(self) -> SCALAR:
        if self.ready:
            self._roll()
            buffer = np.array(self._value)
            return buffer
        else:
            return None

    @value.setter
    def value(self, value: Array[SCALAR]) -> None:
        """This is only for internal use, the times must be set at the same time"""
        if value is None:
            self.invalidate()
            return
        self._value = deque(value)
        self.mark_modified()

    @property
    def delta_value(self) -> Optional[SCALAR]:
        if self.ready and self._times and (tm := self._times[-1]) == self.owning_graph.evaluation_clock.evaluation_time:
            return self._value[-1]
        else:
            return None

    @property
    def value_times(self) -> Array[datetime]:
        self._roll()
        return self._times

    @value_times.setter
    def value_times(self, value: Array[SCALAR]) -> None:
        self._times = deque(value)

    @property
    def min_size(self) -> timedelta:
        return self._min_size

    @property
    def size(self) -> timedelta:
        return self._size

    @property
    def first_modified_time(self) -> datetime:
        return self._times[0] if self._times else MIN_TD

    def invalidate(self):
        self.mark_invalid()

    def can_apply_result(self, result: SCALAR):
        return not self.modified

    def apply_result(self, result: SCALAR):
        if result is None:
            return
        try:
            if HG_TYPE_CHECKING:
                tp_ = origin if (origin := typing.get_origin(self._tp)) else self._tp
                if not isinstance(result, tp_):
                    raise TypeError(f"Expected {self._tp}, got {type(result)}")
            self._value.append(result)
            self._times.append(self.owning_graph.evaluation_clock.evaluation_time)
            self.mark_modified()
        except Exception as e:
            raise TypeError(f"Cannot apply node output {result} of "
                            f"type {result.__class__.__name__} to {self}: {e}") from e

    def clear(self):
        # TODO: what is the right semantics here? Value time series don't do anything, collections remove all items
        pass

    def mark_invalid(self):
        self._value = deque()
        self._times = deque()
        super().mark_invalid()

    def copy_from_output(self, output: "TimeSeriesOutput"):
        assert isinstance(output, PythonTimeSeriesFixedWindowOutput)
        self.value = output._value
        self.value_times = output._times

    def copy_from_input(self, input: "TimeSeriesInput"):
        assert isinstance(input, PythonTimeSeriesWindowInput)
        assert isinstance(input.output, PythonTimeSeriesFixedWindowOutput)
        self.value = input.output._value
        self.value_times = input.output._times

    def __len__(self) -> int:
        self._roll()
        return len(self._value)


@dataclass
class PythonTimeSeriesWindowInput(PythonBoundTimeSeriesInput,
                                  TimeSeriesWindowInput[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN],
                                  Generic[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]):
    """
    The only difference between a PythonBoundTimeSeriesInput and a PythonTimeSeriesValueInput is that the
    signature of value etc.
    """

    @property
    def value_times(self) -> tuple[datetime, ...]:
        output: TimeSeriesWindowOutput[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN] = self.output
        return output.value_times

    @property
    def first_modified_time(self) -> datetime:
        output: TimeSeriesWindowOutput[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN] = self.output
        return output.first_modified_time

    @property
    def min_size(self) -> int | timedelta:
        return self.output.min_size

    @property
    def size(self) -> int | timedelta:
        return self.output.size

    def __len__(self) -> int:
        return len(self.output)

