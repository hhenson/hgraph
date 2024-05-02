from abc import ABC
from typing import Generic

from hgraph._types._time_series_types import TimeSeriesInput, TIME_SERIES_TYPE

__all__ = ("CONTEXT", "REQUIRED", "TimeSeriesContextInput")



class TimeSeriesContextInput(TimeSeriesInput, ABC, Generic[TIME_SERIES_TYPE]):
    """
    This is a placeholder class, it is never supposed to be instantiated or derived from
    """


# Shorthand for a TimeSeriesValueInput
CONTEXT = TimeSeriesContextInput


class REQUIRED:
    def __init__(self, name: str):
        self.name = name

    def __class_getitem__(cls, item):
        assert type(item) is str
        return cls(item)

    def __str__(self):
        return self.name