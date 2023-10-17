from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any, Mapping, Generic, KeysView

from hg import TimeSeriesDeltaValue, TimeSeries
from hg._types._time_series_types import TimeSeriesOutput, TimeSeriesInput, DELTA_SCALAR
from hg._types._tsb_type import TimeSeriesBundleInput, TS_SCHEMA
from hg._types._scalar_value import ScalarValue
from hg._impl._types._input import PythonTimeSeriesInput
from hg._impl._types._output import PythonTimeSeriesOutput


# With Bundles there are two implementation types, namely bound and un-bound.
# Bound bundles are those that are bound to a specific output, and un-bound bundles are those that are not.
# A bound bundle has a peer output of the same shape that this bundle can map directly to. A bound bundle
# has a higher performance characteristic, as it does not need to loop over all the inputs to determine
# things such as active, modified, etc.


class PythonUnboundTimeSeriesBundleInput(PythonTimeSeriesInput, TimeSeriesBundleInput[TS_SCHEMA], Generic[TS_SCHEMA]):

    _ts_value: Mapping[str, PythonTimeSeriesInput]

    def __getattr__(self, item) -> TimeSeries:
        # TODO: Should the _ts_value be a dict or a wrapper to support attribute retrieval?
        if item in self._ts_value:
            return self._ts_value[item]
        else:
            raise ValueError(f"'{item}' is not a valid property of TSB")

    def keys(self) -> KeysView[str]:
        return self._ts_value.keys()

    @property
    def scalar_value(self) -> ScalarValue:
        pass

    @property
    def delta_scalar_value(self) -> ScalarValue:
        pass

    @property
    def bound(self) -> bool:
        return False

    @property
    def output(self) -> Optional[TimeSeriesOutput]:
        return None

    @property
    def value(self) -> Any:
        return super().value

    @property
    def active(self) -> bool:
        return any(ts.active for ts in self._ts_value.values())

    def make_active(self):
        for ts in self._ts_value.values():
            ts.make_active()

    def make_passive(self):
        for ts in self._ts_value.values():
            ts.make_passive()

    @property
    def delta_value(self) -> Mapping[str, Any]:
        # TODO: Sort out how to describe value and delta value in a more consistent way. All time-series instances
        # have a notion of value. But They may not always have a simple notion of delta, perhaps we should only
        # support the scalar value notion of value and delta value? In python we don't actually need the notion
        # of scalar value as Python values are effectively any's anyway, so it is really on the C++
        # implementations that need this.
        return {k: ts.delta_value if isinstance(ts, TimeSeriesDeltaValue) else ts.value for k, ts in self._ts_value.items()}

    @property
    def modified(self) -> bool:
        return any(ts.modified for ts in self._ts_value.values())

    @property
    def valid(self) -> bool:
        return any(ts.valid for ts in self._ts_value.values())

    @property
    def all_valid(self) -> bool:
        return all(ts.valid for ts in self._ts_value.values())

    @property
    def last_modified_time(self) -> datetime:
        return max(ts.last_modified_time for ts in self._ts_value.values())