from typing import Type, TypeVar, Optional, _GenericAlias


__all__ = ("HgCONTEXTTypeMetaData",)

from hgraph._types._type_meta_data import ParseError, HgTypeMetaData
from hgraph._types._tsb_meta_data import HgTimeSeriesTypeMetaData


class HgCONTEXTTypeMetaData(HgTimeSeriesTypeMetaData):
    """Parses CONTEXT[TS[...]]"""

    value_tp: HgTimeSeriesTypeMetaData
    is_context: bool = True

    def __init__(self, value_type: HgTimeSeriesTypeMetaData):
        self.value_tp = value_type

    @property
    def is_resolved(self) -> bool:
        return self.value_tp.is_resolved

    @property
    def py_type(self) -> Type:
        from hgraph._types._context_type import CONTEXT
        return CONTEXT[self.value_tp.py_type]

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return type(self)(self.value_tp.resolve(resolution_dict, weak))

    def matches(self, tp: "HgTypeMetaData") -> bool:
        if isinstance(tp, HgCONTEXTTypeMetaData):
            return self.value_tp.matches(tp.value_tp)
        else:
            return self.value_tp.matches(tp.dereference())

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if isinstance(wired_type, HgCONTEXTTypeMetaData):
            self.value_tp.build_resolution_dict(resolution_dict, wired_type.value_tp)
        else:
            self.value_tp.build_resolution_dict(resolution_dict, wired_type if wired_type else None)

    def build_resolution_dict_from_scalar(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"],
                                          wired_type: "HgTypeMetaData", value: object):
        self.value_tp.build_resolution_dict_from_scalar(resolution_dict, wired_type, value)

    def scalar_type(self) -> "HgScalarTypeMetaData":
        return self.value_tp.scalar_type()

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._context_type import TimeSeriesContextInput
        if isinstance(value_tp, _GenericAlias) and value_tp.__origin__ is TimeSeriesContextInput:
            value_tp = HgTimeSeriesTypeMetaData.parse_type(value_tp.__args__[0])
            if value_tp is None:
                raise ParseError(f"While parsing 'CONTEXT[{str(value_tp.__args__[0])}]' unable to parse time series type from '{str(value_tp.__args__[0])}'")
            if value_tp.has_references:
                raise ParseError(f"While parsing 'CONTEXT[{str(value_tp.__args__[0])}]': time series type must not have references")
            return HgCONTEXTTypeMetaData(value_tp)

    @property
    def has_references(self) -> bool:
        return False

    def dereference(self) -> "HgTimeSeriesTypeMetaData":
        return self

    @property
    def operator_rank(self) -> float:
        return self.value_tp.operator_rank

    def __getitem__(self, item):
        return self.value_tp[item]

    def __eq__(self, o: object) -> bool:
        return type(o) is HgCONTEXTTypeMetaData and self.value_tp == o.value_tp

    def __str__(self) -> str:
        return f'CONTEXT[{str(self.value_tp)}]'

    def __repr__(self) -> str:
        return f'HgCONTEXTTypeMetaData({repr(self.value_tp)})'

    def __hash__(self) -> int:
        from hgraph._types._ref_type import REF
        return hash(REF) ^ hash(self.value_tp)
