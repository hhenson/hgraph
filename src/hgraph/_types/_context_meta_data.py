from types import NoneType
from typing import Type, TypeVar, Optional, _GenericAlias, Mapping

__all__ = ("HgCONTEXTTypeMetaData",)

from hgraph._types._type_meta_data import ParseError, HgTypeMetaData
from hgraph._types._tsb_meta_data import HgTimeSeriesTypeMetaData


class HgCONTEXTTypeMetaData(HgTimeSeriesTypeMetaData):
    """Parses CONTEXT[...]"""

    value_tp: HgTypeMetaData
    is_context_wired: bool = True
    is_context_manager: bool = False

    def __init__(self, value_type: HgTypeMetaData):
        self.value_tp = value_type

        if value_type.is_scalar:
            scalar_py_type = value_type.py_type
        else:
            scalar_py_type = value_type.scalar_type().py_type if value_type.is_resolved else NoneType

        self.is_context_manager = getattr(scalar_py_type, "__enter__", None) and getattr(
            scalar_py_type, "__exit__", None
        )

    @property
    def is_resolved(self) -> bool:
        return self.value_tp.is_resolved and not self.value_tp.is_scalar

    @property
    def py_type(self) -> Type:
        from hgraph._types._context_type import CONTEXT

        return CONTEXT[self.value_tp.py_type]

    @property
    def ts_type(self):
        from hgraph import HgTSTypeMetaData

        return HgTSTypeMetaData(self.value_tp) if self.value_tp.is_scalar else self.value_tp

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        elif self.value_tp.is_scalar:
            return resolution_dict[self] if self in resolution_dict else self
        else:
            return type(self)(self.value_tp.resolve(resolution_dict, weak))

    def matches(self, tp: "HgTypeMetaData") -> bool:
        if isinstance(tp, HgCONTEXTTypeMetaData):
            return self.value_tp.matches(tp.value_tp)
        elif self.value_tp.is_scalar:
            if tp.is_scalar:
                return self.value_tp.matches(tp)
            else:
                return self.value_tp.matches(tp.scalar_type())
        else:
            return self.value_tp.matches(tp.dereference())

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if isinstance(wired_type, HgCONTEXTTypeMetaData):
            self.value_tp.build_resolution_dict(resolution_dict, wired_type.value_tp)
        elif self.value_tp.is_scalar:
            resolution_dict[self] = wired_type
        else:
            self.value_tp.build_resolution_dict(resolution_dict, wired_type if wired_type else None)

    def build_resolution_dict_from_scalar(
        self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData", value: object
    ):
        if not self.value_tp.is_scalar:
            self.value_tp.build_resolution_dict_from_scalar(resolution_dict, wired_type, value)
        else:
            self.value_tp.build_resolution_dict(resolution_dict, wired_type)

    def scalar_type(self) -> "HgScalarTypeMetaData":
        return self.value_tp if self.value_tp.is_scalar else self.value_tp.scalar_type()

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._context_type import TimeSeriesContextInput

        if isinstance(value_tp, _GenericAlias) and value_tp.__origin__ is TimeSeriesContextInput:
            value_tp = HgTypeMetaData.parse_type(value_tp.__args__[0])
            if value_tp is None:
                raise ParseError(
                    f"While parsing 'CONTEXT[{str(value_tp.__args__[0])}]' unable to parse time series type from '{str(value_tp.__args__[0])}'"
                )
            if value_tp.has_references:
                raise ParseError(
                    f"While parsing 'CONTEXT[{str(value_tp.__args__[0])}]': time series type must not have references"
                )
            return HgCONTEXTTypeMetaData(value_tp)

    @property
    def has_references(self) -> bool:
        return False

    def dereference(self) -> "HgTimeSeriesTypeMetaData":
        return self

    @property
    def typevars(self):
        return self.value_tp.typevars

    @property
    def generic_rank(self) -> dict[type, float]:
        return self.value_tp.generic_rank

    def __getitem__(self, item):
        return self.value_tp[item]

    def __eq__(self, o: object) -> bool:
        return type(o) is HgCONTEXTTypeMetaData and self.value_tp == o.value_tp

    def __str__(self) -> str:
        return f"CONTEXT[{str(self.value_tp)}]"

    def __repr__(self) -> str:
        return f"HgCONTEXTTypeMetaData({repr(self.value_tp)})"

    def __hash__(self) -> int:
        from hgraph._types._ref_type import REF

        return hash(REF) ^ hash(self.value_tp)
