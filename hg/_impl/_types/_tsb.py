from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Generic, KeysView, ItemsView, ValuesView

from hg._impl._types._input import PythonTimeSeriesInput, PythonBoundTimeSeriesInput
from hg._impl._types._output import PythonTimeSeriesOutput
from hg._types._time_series_types import TimeSeriesOutput, TimeSeriesInput, TimeSeries
from hg._types._tsb_type import TimeSeriesBundleInput, TS_SCHEMA, TimeSeriesBundleOutput


# With Bundles there are two implementation types, namely bound and un-bound.
# Bound bundles are those that are bound to a specific output, and un-bound bundles are those that are not.
# A bound bundle has a peer output of the same shape that this bundle can map directly to. A bound bundle
# has a higher performance characteristic, as it does not need to loop over all the inputs to determine
# things such as active, modified, etc.

@dataclass
class PythonTimeSeriesBundleOutput(PythonTimeSeriesOutput, TimeSeriesBundleOutput[TS_SCHEMA], Generic[TS_SCHEMA]):
    _ts_value: Mapping[str, PythonTimeSeriesOutput] = field(default_factory=dict)

    @property
    def value(self):
        return {k: ts.value for k, ts in self._ts_value.items() if ts.valid}

    @property
    def delta_value(self):
        return {k: ts.delta_value for k, ts in self._ts_value.items() if ts.modified}

    def __getattr__(self, item) -> TimeSeries:
        if item in self._ts_value:
            return self._ts_value[item]
        else:
            raise ValueError(f"'{item}' is not a valid property of TSB")

    def keys(self) -> KeysView[str]:
        return self._ts_value.keys()

    def items(self) -> ItemsView[str, TimeSeries]:
        return self._ts_value.items()

    def values(self) -> ValuesView[TimeSeries]:
        return self._ts_value.values()

    def apply_result(self, value: Mapping[str, Any]):
        for k, v in value.items():
            self._ts_value[k].apply_result(v)

    def copy_from_output(self, output: "TimeSeriesOutput"):
        if not isinstance(output, PythonTimeSeriesBundleOutput):
            raise TypeError(f"Expected {PythonTimeSeriesBundleOutput}, got {type(output)}")
        # TODO: Put in some validation that the signatures are compatible?
        for k, v in output._ts_value.items():
            self._ts_value[k].copy_from_output(v)

    def copy_from_input(self, input: "TimeSeriesInput"):
        if not isinstance(input, TimeSeriesBundleInput):
            raise TypeError(f"Expected {TimeSeriesBundleInput}, got {type(input)}")
        for k, v in input.items():
            self._ts_value[k].copy_from_input(v)

    def mark_invalid(self):
        if self.valid:
            for v in self._ts_value.values():
                v.mark_invalid()
            super().mark_invalid()

    @property
    def all_valid(self) -> bool:
        return all(ts.valid for ts in self._ts_value.values())


class PythonTimeSeriesBundleInput(PythonBoundTimeSeriesInput, TimeSeriesBundleInput[TS_SCHEMA], Generic[TS_SCHEMA]):
    """
    The bound TSB has a corresponding peer output that it is bound to.
    This means most all methods can be delegated to the output. This is slightly more efficient than the unbound version.
    """

    _ts_value: Mapping[str, PythonTimeSeriesInput]

    def __getattr__(self, item) -> TimeSeries:
        # TODO: Should the _ts_value be a dict or a wrapper to support attribute retrieval?
        if item in self._ts_value:
            return self._ts_value[item]
        else:
            raise ValueError(f"'{item}' is not a valid property of TSB")

    def keys(self) -> KeysView[str]:
        return self._ts_value.keys()

    def items(self) -> ItemsView[str, TimeSeries]:
        return self._ts_value.items()

    def values(self) -> ValuesView[TimeSeries]:
        return self._ts_value.values()

    @property
    def all_valid(self) -> bool:
        return all(ts.valid for ts in self._ts_value.values())

    @property
    def has_peer(self) -> bool:
        return super().bound

    @property
    def bound(self) -> bool:
        return super().bound or any(ts.bound for ts in self._ts_value.values())

    def bind_output(self, output: TimeSeriesOutput):
        output: PythonTimeSeriesBundleOutput
        super().bind_output(output)
        for k, ts in self._ts_value.items():
            ts.bind_output(output[k])

    @property
    def value(self) -> Any:
        if self.has_peer:
            return super().value
        else:
            return {K: ts.value for K, ts in self._ts_value.items() if ts.valid}

    @property
    def delta_value(self) -> Mapping[str, Any]:
        if self.has_peer:
            return super().delta_value
        else:
            return {k: ts.delta_value for k, ts in self._ts_value.items() if ts.modified}

    @property
    def active(self) -> bool:
        """
        For UnBound TS, if any of the elements are active we report the input as active,
        Note, that make active / make passive will make ALL instances active / passive.
        Thus, just because the input returns True for active, it does not mean that make_active is a no-op.
        """
        if self.has_peer:
            return super().active
        else:
            return any(ts.active for ts in self._ts_value.values())

    def make_active(self):
        if self.has_peer:
            super().make_active()
        else:
            for ts in self._ts_value.values():
                ts.make_active()

    def make_passive(self):
        if self.has_peer:
            super().make_passive()
        else:
            for ts in self._ts_value.values():
                ts.make_passive()

    @property
    def modified(self) -> bool:
        if self.has_peer:
            return super().modified
        else:
            return any(ts.modified for ts in self._ts_value.values())

    @property
    def valid(self) -> bool:
        if self.has_peer:
            return super().valid
        else:
            return any(ts.valid for ts in self._ts_value.values())

    @property
    def last_modified_time(self) -> datetime:
        if self.has_peer:
            return super().last_modified_time
        else:
            return max(ts.last_modified_time for ts in self._ts_value.values())
