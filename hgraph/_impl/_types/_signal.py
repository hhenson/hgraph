from dataclasses import dataclass
from datetime import datetime

from hgraph._types import TimeSeriesInput
from hgraph._impl._types._input import PythonBoundTimeSeriesInput
from hgraph._types._time_series_types import TimeSeriesSignalInput


@dataclass
class PythonTimeSeriesSignal(PythonBoundTimeSeriesInput, TimeSeriesSignalInput):
    """The TimeSeriesSignal differs in behaviour to PythonBoundTimeSeriesInput in that it's value is always True."""

    _ts_values: list["PythonTimeSeriesSignal"] = None
    _impl: TimeSeriesInput = None

    @property
    def value(self) -> bool:
        return True

    @property
    def delta_value(self) -> bool:
        return True

    def __getitem__(self, index):
        #  this signal has been bound to a free bundle or a TSL so will be bound item-wise
        if self._impl is not None:
            return self._impl[index]
        if self._ts_values is None:
            self._ts_values = []
        while index > len(self._ts_values) - 1:
            new_item = PythonTimeSeriesSignal(_parent_or_node=self)
            if self.active:
                new_item.make_active()
            self._ts_values.append(new_item)
        return self._ts_values[index]

    def bind_output(self, output):
        if self._impl is not None:
            return self._impl.bind_output(output)
        else:
            return super().bind_output(output)

    def un_bind_output(self, unbind_refs = False):
        if self._impl is not None:
            return self._impl.un_bind_output(unbind_refs)
        else:
            return super().un_bind_output(unbind_refs)

    def do_un_bind_output(self, unbind_refs: bool = False):
        if self._output is not None:  # signal bound to a free standing bundle/TSL will not have an _output 
            super().do_un_bind_output(unbind_refs=unbind_refs)
        if self._ts_values is not None:  # signal attached to any peer time series will not have items
            for item in self._ts_values:
                item.do_un_bind_output(unbind_refs=unbind_refs)
            self._ts_values = None

    @property
    def bound(self) -> bool:
        if self._impl is not None:
            return self._impl.bound
        else:
            return self._output is not None or (self._ts_values is not None and len(self._ts_values) > 0)

    def make_active(self):
        super().make_active()
        if self._impl is not None:
            self._impl.make_active()
        elif self._ts_values:
            for item in self._ts_values:
                item.make_active()

    def make_passive(self):
        super().make_passive()
        if self._impl is not None:
            self._impl.make_passive()
        elif self._ts_values:
            for item in self._ts_values:
                item.make_passive()

    @property
    def valid(self) -> bool:
        if self._impl is not None:
            return self._impl.valid
        elif self._ts_values:
            return any(item.valid for item in self._ts_values)
        else:
            return super().valid

    @property
    def modified(self) -> bool:
        if self._impl is not None:
            return self._impl.modified
        elif self._ts_values:
            return any(item.modified for item in self._ts_values)
        else:
            return super().modified

    @property
    def last_modified_time(self) -> datetime:
        if self._impl is not None:
            return self._impl.last_modified_time
        elif self._ts_values:
            return max(item.last_modified_time for item in self._ts_values)
        else:
            return super().last_modified_time
