from dataclasses import dataclass
from datetime import datetime

from hgraph._impl._types._input import PythonBoundTimeSeriesInput
from hgraph._types._time_series_types import TimeSeriesSignalInput


@dataclass
class PythonTimeSeriesSignal(PythonBoundTimeSeriesInput, TimeSeriesSignalInput):
    """The TimeSeriesSignal differs in behaviour to PythonBoundTimeSeriesInput in that it's value is always True."""

    _ts_values: list["PythonTimeSeriesSignal"] = None

    @property
    def value(self) -> bool:
        return True

    @property
    def delta_value(self) -> bool:
        return True

    def __getitem__(self, index):
        #  this signal has been bound to a free bundle or a TSL so will be bound item-wise
        if self._ts_values is None:
            self._ts_values = []
        while index > len(self._ts_values) - 1:
            new_item = PythonTimeSeriesSignal(_owning_node=self._owning_node, _parent_input=self)
            self._ts_values.append(new_item)
        return self._ts_values[index]

    def do_un_bind_output(self):
        if self._ts_values is not None:
            for item in self._ts_values:
                item.do_un_bind_output()

    def make_active(self):
        super().make_active()
        if self._ts_values:
            for item in self._ts_values:
                item.make_active()

    def make_passive(self):
        super().make_passive()
        if self._ts_values:
            for item in self._ts_values:
                item.make_passive()

    @property
    def valid(self) -> bool:
        if self._ts_values:
            return any(item.valid for item in self._ts_values)
        else:
            return super().valid

    @property
    def modified(self) -> bool:
        if self._ts_values:
            return any(item.modified for item in self._ts_values)
        else:
            return super().modified

    @property
    def last_modified_time(self) -> datetime:
        if self._ts_values:
            return max(item.last_modified_time for item in self._ts_values)
        else:
            return super().last_modified_time
