import itertools
from functools import reduce
from statistics import fmean
from typing import Type, TypeVar, Optional, Sequence

from hgraph._types._time_series_types import TimeSeries
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

    def __init__(self, py_type, constraints: Sequence[HgTimeSeriesTypeMetaData] = ()):
        self.py_type = py_type
        self.constraints = constraints

    def matches(self, tp: "HgTypeMetaData") -> bool:
        if isinstance(tp, HgTsTypeVarTypeMetaData):
            if self.py_type == tp.py_type:
                return True
            for s_i, tp_i in itertools.product(self.constraints, tp.constraints):
                s_t = isinstance(s_i, HgTimeSeriesTypeMetaData)
                tp_t = isinstance(tp_i, HgTimeSeriesTypeMetaData)
                if s_t and tp_t:
                    if s_i.matches(tp_i):
                        return True
                if not s_t and tp_t:
                    if issubclass(getattr(tp.py_type, "__origin__", tp.py_type), s_i):
                        return True
                if not s_t and not tp_t:
                    if issubclass(tp_i, s_i):
                        return True
        elif not tp.is_scalar:
            return any(
                (
                    c.matches(tp)
                    if isinstance(c, HgTimeSeriesTypeMetaData)
                    else issubclass(getattr(tp.py_type, "__origin__", tp.py_type), c)
                )
                for c in self.constraints
            )
        else:
            return any(issubclass(tp.py_type, c) for c in self.constraints)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if tp := resolution_dict.get(self.py_type):
            return tp
        elif not weak:
            raise ParseError(f"No resolution available for '{str(self)}'")
        else:
            return self

    @property
    def type_vars(self):
        return {self.py_type}

    @property
    def generic_rank(self) -> dict[type, float]:
        # A complete wild card, will have a rank of 1. however one with constraints will have a lower rank so we can
        # discriminate between typevars with different constraints

        avg_constraints_rank = fmean(
            itertools.chain(*(
                c.generic_rank.values() if isinstance(c, HgTimeSeriesTypeMetaData) else [1.0] for c in self.constraints
            ))
        )

        return {self.py_type: 0.9 + avg_constraints_rank / 10.0}

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if wired_type.is_scalar and not issubclass(wired_type.py_type, TimeSeries):
            raise ParseError(f"TimeSeries TypeVar '{str(self)}' does not match scalar type: '{str(wired_type)}'")
        if self.py_type in resolution_dict:
            match = resolution_dict[self.py_type]
            if not match.matches(wired_type):
                raise ParseError(
                    f"TypeVar '{str(self)}' has already been resolved to"
                    f" '{str(resolution_dict[self.py_type])}' which does not match the type "
                    f"'{str(wired_type)}'"
                )
        elif wired_type != self:
            resolution_dict[self.py_type] = wired_type

    def build_resolution_dict_from_scalar(
        self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData", value: object
    ):
        from hgraph._types._ts_meta_data import HgTSTypeMetaData

        resolved_type = HgTSTypeMetaData(wired_type)
        if self.py_type in resolution_dict:
            if not resolution_dict[self.py_type].matches(resolved_type):
                raise ParseError(
                    f"TypeVar '{str(self)}' has already been resolved to"
                    f" '{str(resolution_dict[self.py_type])}' which does not match the type "
                    f"'{str(wired_type)}'"
                )
        elif wired_type.is_resolved:
            resolution_dict[self.py_type] = resolved_type

    def scalar_type(self) -> "HgScalarTypeMetaData":
        raise ValueError(f"Time series TypeVars do not have a scalar type equivalent: {str(self)}")

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._time_series_types import TimeSeries
        from hgraph._types._tsb_type import TimeSeriesSchema

        if isinstance(value_tp, TypeVar):
            if value_tp.__bound__ and issubclass(
                getattr(value_tp.__bound__, "__origin__", value_tp.__bound__), (TimeSeries, TimeSeriesSchema)
            ):
                return HgTsTypeVarTypeMetaData(
                    value_tp, (HgTimeSeriesTypeMetaData.parse_type(value_tp.__bound__) or value_tp.__bound__,)
                )
            elif value_tp.__constraints__ and all(
                not HgTypeMetaData.parse_type(c).is_scalar for c in value_tp.__constraints__
            ):
                return HgTsTypeVarTypeMetaData(
                    value_tp, tuple(HgTimeSeriesTypeMetaData.parse_type(t) or t for t in value_tp.__constraints__)
                )
        return None

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTsTypeVarTypeMetaData and self.py_type == o.py_type

    def __str__(self) -> str:
        return self.py_type.__name__

    def __repr__(self) -> str:
        return f"HgTsTypeVarTypeMetaData({repr(self.py_type)})"

    def __hash__(self) -> int:
        return hash(self.py_type)
