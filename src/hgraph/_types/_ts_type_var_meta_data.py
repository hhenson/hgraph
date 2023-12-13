from typing import Type, TypeVar, Optional

from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._type_meta_data import ParseError, HgTypeMetaData

__all__ = ("HgTsTypeVarTypeMetaData",)


class HgTsTypeVarTypeMetaData(HgTimeSeriesTypeMetaData):
    """
    Represent a time series type var, for example TIME_SERIES_TYPE.
    The bound of the TypeVar must be of type TimeSeries or TimeSeriesSchema.
    """

    py_type: Type
    is_generic: bool = True
    is_resolved: bool = False

    def __init__(self, py_type):
        self.py_type = py_type

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return not tp.is_scalar

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if tp := resolution_dict.get(self.py_type):
            return tp
        elif not weak:
            raise ParseError(f"No resolution available for '{str(self)}'")
        else:
            return self

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if wired_type.is_scalar:
            raise ParseError(f"TimeSeries TypeVar '{str(self)}' does not match scalar type: '{str(wired_type)}'")
        if self in resolution_dict:
            if resolution_dict[self.py_type] != wired_type:
                raise ParseError(f"TypeVar '{str(self)}' has already been resolved to"
                                 f" '{str(resolution_dict[self])}' which does not match the type "
                                 f"'{str(wired_type)}'")
        elif wired_type != self:
            resolution_dict[self.py_type] = wired_type

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        from hgraph._types._time_series_types import TimeSeries
        from hgraph._types._tsb_type import TimeSeriesSchema
        if isinstance(value, TypeVar) and value.__bound__ and issubclass(value.__bound__, (TimeSeries, TimeSeriesSchema)):
            return HgTsTypeVarTypeMetaData(value)
        return None

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTsTypeVarTypeMetaData and self.py_type == o.py_type

    def __str__(self) -> str:
        return self.py_type.__name__

    def __repr__(self) -> str:
        return f'HgTsTypeVarTypeMetaData({repr(self.py_type)})'

    def __hash__(self) -> int:
        return hash(self.py_type)
