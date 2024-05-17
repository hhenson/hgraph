from typing import Type, TypeVar, Optional, _GenericAlias, Dict

from hgraph._types._tsd_type import KEY_SET_ID
from hgraph._types._tss_meta_data import HgTSSTypeMetaData
from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData, HgDictScalarType
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData, HgTypeMetaData


__all__ = ("HgTSDTypeMetaData", "HgTSDOutTypeMetaData",)


class HgTSDTypeMetaData(HgTimeSeriesTypeMetaData):

    key_tp: HgScalarTypeMetaData
    value_tp: HgTimeSeriesTypeMetaData

    def __init__(self, key_tp: HgScalarTypeMetaData, value_tp: HgTimeSeriesTypeMetaData):
        self.value_tp = value_tp
        self.key_tp = key_tp

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return isinstance(tp, HgTSDTypeMetaData) and self.key_tp.matches(tp.key_tp) and self.value_tp.matches(tp.value_tp)

    @property
    def is_resolved(self) -> bool:
        return self.value_tp.is_resolved and self.key_tp.is_resolved

    @property
    def py_type(self) -> Type:
        from hgraph._types._tsd_type import TSD
        return TSD[self.key_tp.py_type, self.value_tp.py_type]

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return type(self)(self.key_tp.resolve(resolution_dict, weak), self.value_tp.resolve(resolution_dict, weak))

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        wired_type: HgTSDTypeMetaData
        self.value_tp.build_resolution_dict(resolution_dict, wired_type.value_tp)
        self.key_tp.build_resolution_dict(resolution_dict, wired_type.key_tp)

    def build_resolution_dict_from_scalar(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"],
                                          wired_type: "HgTypeMetaData", value: object):
        if isinstance(wired_type, HgDictScalarType):
            value: Dict
            k, v = next(iter(value.items()))
            self.key_tp.do_build_resolution_dict(resolution_dict, HgTypeMetaData.parse_value(k))
            self.value_tp.build_resolution_dict_from_scalar(resolution_dict, HgTypeMetaData.parse_value(v), v)
        else:
            super().build_resolution_dict_from_scalar(resolution_dict, wired_type, value)

    def scalar_type(self) -> "HgScalarTypeMetaData":
        return HgDictScalarType(self.key_tp, self.value_tp.scalar_type())

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._tsd_type import TimeSeriesDictInput
        from hgraph._types._type_meta_data import ParseError
        if isinstance(value_tp, _GenericAlias) and value_tp.__origin__ is TimeSeriesDictInput:
            key_tp = HgScalarTypeMetaData.parse_type(value_tp.__args__[0])
            if key_tp is None:
                raise ParseError(f"Could not parse key type {value_tp.__args__[0]} when parsing {value_tp}")
            value_tp = HgTimeSeriesTypeMetaData.parse_type(value_tp.__args__[1])
            if value_tp is None:
                raise ParseError(f"Could not parse value type {value_tp.__args__[1]} when parsing {value_tp}")
            return HgTSDTypeMetaData(key_tp, value_tp)

    @property
    def has_references(self) -> bool:
        return self.value_tp.has_references

    def dereference(self) -> "HgTimeSeriesTypeMetaData":
        if self.has_references:
            return self.__class__(self.key_tp, self.value_tp.dereference())
        else:
            return self

    @property
    def typevars(self):
        return self.key_tp.typevars + self.value_tp.typevars

    @property
    def operator_rank(self) -> float:
        return (self.key_tp.operator_rank + self.value_tp.operator_rank) / 100.

    def __getitem__(self, item):
        if KEY_SET_ID is item:
            return HgTSSTypeMetaData(self.key_tp)
        else:
            return self.value_tp

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTSDTypeMetaData and self.key_tp == o.key_tp and self.value_tp == o.value_tp

    def __str__(self) -> str:
        return f'TSD[{str(self.key_tp)}, {str(self.value_tp)}]'

    def __repr__(self) -> str:
        return f'HgTSDTypeMetaData({repr(self.key_tp)}, {repr(self.value_tp)})'

    def __hash__(self) -> int:
        from hgraph._types._tsd_type import TSD
        return hash(TSD) ^ hash(self.value_tp) ^ hash(self.key_tp)


class HgTSDOutTypeMetaData(HgTSDTypeMetaData):

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._tsd_type import TimeSeriesDictOutput
        if isinstance(value_tp, _GenericAlias) and value_tp.__origin__ is TimeSeriesDictOutput:
            return HgTSDOutTypeMetaData(HgScalarTypeMetaData.parse_type(value_tp.__args__[0]),
                                        HgTimeSeriesTypeMetaData.parse_type(value_tp.__args__[1]))

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTSDOutTypeMetaData and self.key_tp == o.key_tp and self.value_tp == o.value_tp

    def __str__(self) -> str:
        return f'TSD_OUT[{str(self.key_tp)}, {str(self.value_tp)}]'

    def __repr__(self) -> str:
        return f'HgTSDOutTypeMetaData({repr(self.key_tp)}, {repr(self.value_tp)})'
