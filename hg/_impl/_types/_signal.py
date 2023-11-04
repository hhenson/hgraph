from dataclasses import dataclass

from hg._impl._types._input import PythonBoundTimeSeriesInput
from hg._types._time_series_types import TimeSeriesSignalInput


@dataclass
class PythonTimeSeriesSignal(PythonBoundTimeSeriesInput, TimeSeriesSignalInput):
    """The TimeSeriesSignal differs in behaviour to PythonBoundTimeSeriesInput in that it's value is always True."""

    @property
    def value(self) -> bool:
        return True

    @property
    def delta_value(self) -> bool:
        return True
