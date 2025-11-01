from typing import Type, TypeVar, Optional, _GenericAlias, Mapping

__all__ = (
    "HgREFTypeMetaData",
    "HgREFOutTypeMetaData",
)

from hgraph._types._type_meta_data import ParseError, HgTypeMetaData
from hgraph._types._tsb_meta_data import HgTimeSeriesTypeMetaData


class HgREFTypeMetaData(HgTimeSeriesTypeMetaData):
    """Parses TS[...]"""

    value_tp: HgTimeSeriesTypeMetaData
    is_reference: bool = True

    def __init__(self, value_type: HgTimeSeriesTypeMetaData):
        self.value_tp = value_type

    @property
    def is_resolved(self) -> bool:
        return self.value_tp.is_resolved

    @property
    def py_type(self) -> Type:
        from hgraph._types._ref_type import REF

        return REF[self.value_tp.py_type]

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return type(self)(self.value_tp.resolve(resolution_dict, weak))

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return self.value_tp.matches(tp.dereference())

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if isinstance(wired_type, HgREFTypeMetaData):
            self.value_tp.build_resolution_dict(resolution_dict, wired_type.value_tp)
        else:
            self.value_tp.build_resolution_dict(resolution_dict, wired_type if wired_type else None)

    def build_resolution_dict_from_scalar(
        self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData", value: object
    ):
        self.value_tp.build_resolution_dict_from_scalar(resolution_dict, wired_type, value)

    def scalar_type(self) -> "HgScalarTypeMetaData":
        from hgraph._types._ref_type import TimeSeriesReference

        return HgTypeMetaData.parse_type(TimeSeriesReference)

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._ref_type import TimeSeriesReferenceInput

        if isinstance(value_tp, _GenericAlias) and value_tp.__origin__ is TimeSeriesReferenceInput:
            value_tp = HgTimeSeriesTypeMetaData.parse_type(value_tp.__args__[0])
            if value_tp is None:
                raise ParseError(
                    f"While parsing 'REF[{str(value_tp.__args__[0])}]' unable to parse time series type from"
                    f" '{str(value_tp.__args__[0])}'"
                )
            return HgREFTypeMetaData(value_tp)

    @property
    def has_references(self) -> bool:
        return True

    def dereference(self) -> "HgTimeSeriesTypeMetaData":
        return self.value_tp.dereference()

    @property
    def type_vars(self):
        return self.value_tp.type_vars

    @property
    def generic_rank(self) -> dict[type, float]:
        return self.value_tp.generic_rank

    def __getitem__(self, item):
        return self.value_tp[item]

    def __eq__(self, o: object) -> bool:
        return type(o) is HgREFTypeMetaData and self.value_tp == o.value_tp

    def __str__(self) -> str:
        return f"REF[{str(self.value_tp)}]"

    def __repr__(self) -> str:
        return f"HgREFTypeMetaData({repr(self.value_tp)})"

    def __hash__(self) -> int:
        from hgraph._types._ref_type import REF

        return hash(REF) ^ hash(self.value_tp)


class HgREFOutTypeMetaData(HgREFTypeMetaData):
    """Parses REFOut[...]"""

    def dereference(self) -> "HgTimeSeriesTypeMetaData":
        return self.value_tp

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._ref_type import TimeSeriesReferenceOutput

        if isinstance(value_tp, _GenericAlias) and value_tp.__origin__ is TimeSeriesReferenceOutput:
            return HgREFOutTypeMetaData(HgTimeSeriesTypeMetaData.parse_type(value_tp.__args__[0]))

    def __eq__(self, o: object) -> bool:
        return type(o) is HgREFOutTypeMetaData and self.value_tp == o.value_tp

    def __str__(self) -> str:
        return f"REF_OUT[{str(self.value_tp)}]"

    def __repr__(self) -> str:
        return f"HgREFOutTypeMetaData({repr(self.value_tp)})"
