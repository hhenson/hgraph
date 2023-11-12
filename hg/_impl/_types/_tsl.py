from typing import Generic, Iterable, Tuple, Optional

from hg import K, V, DELTA_SCALAR
from hg._impl._types._input import PythonBoundTimeSeriesInput
from hg._impl._types._output import PythonTimeSeriesOutput
from hg._types._time_series_types import TimeSeriesOutput, TimeSeriesInput, TIME_SERIES_TYPE
from hg._types._scalar_types import SIZE
from hg._types._tsl_type import TimeSeriesListInput, TimeSeriesListOutput

__all__ = ("PythonTimeSeriesListOutput", "PythonTimeSeriesListInput")


class PythonTimeSeriesListOutput(PythonTimeSeriesOutput, TimeSeriesListOutput[TIME_SERIES_TYPE, SIZE],
                                 Generic[TIME_SERIES_TYPE, SIZE]):

    def __init__(self, __type__: TIME_SERIES_TYPE, __size__: SIZE, *args, **kwargs):
        Generic.__init__(self)
        TimeSeriesListInput.__init__(self, __type__, __size__)
        PythonTimeSeriesOutput.__init__(self, *args, **kwargs)

    def __getitem__(self, item: int) -> TIME_SERIES_TYPE:
        pass

    def __iter__(self) -> Iterable[TIME_SERIES_TYPE]:
        pass

    def keys(self) -> Iterable[K]:
        pass

    def values(self) -> Iterable[V]:
        pass

    def items(self) -> Iterable[Tuple[K, V]]:
        pass

    def modified_keys(self) -> Iterable[K]:
        pass

    def modified_values(self) -> Iterable[V]:
        pass

    def modified_items(self) -> Iterable[Tuple[K, V]]:
        pass

    def valid_keys(self) -> Iterable[K]:
        pass

    def valid_values(self) -> Iterable[V]:
        pass

    def valid_items(self) -> Iterable[Tuple[K, V]]:
        pass

    @property
    def delta_value(self) -> Optional[DELTA_SCALAR]:
        pass

    def apply_result(self, Any):
        pass

    def copy_from_output(self, output: "TimeSeriesOutput"):
        pass

    def copy_from_input(self, input: "TimeSeriesInput"):
        pass


class PythonTimeSeriesListInput(PythonBoundTimeSeriesInput, TimeSeriesListInput[TIME_SERIES_TYPE, SIZE],
                                Generic[TIME_SERIES_TYPE, SIZE]):

    def __init__(self, __type__: TIME_SERIES_TYPE, __size__: SIZE, _owning_node: "Node" = None,
                 _parent_input: "TimeSeriesInput" = None):
        Generic.__init__(self)
        TimeSeriesListInput.__init__(self, __type__, __size__)
        PythonBoundTimeSeriesInput.__init__(self, _owning_node=_owning_node, _parent_input=_parent_input)

    def __getitem__(self, item: int) -> TIME_SERIES_TYPE:
        pass

    def __iter__(self) -> Iterable[TIME_SERIES_TYPE]:
        pass

    def keys(self) -> Iterable[K]:
        pass

    def values(self) -> Iterable[V]:
        pass

    def items(self) -> Iterable[Tuple[K, V]]:
        pass

    def modified_keys(self) -> Iterable[K]:
        pass

    def modified_values(self) -> Iterable[V]:
        pass

    def modified_items(self) -> Iterable[Tuple[K, V]]:
        pass

    def valid_keys(self) -> Iterable[K]:
        pass

    def valid_values(self) -> Iterable[V]:
        pass

    def valid_items(self) -> Iterable[Tuple[K, V]]:
        pass
