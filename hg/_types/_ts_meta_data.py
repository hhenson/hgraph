from typing import Type, TypeVar, Optional, _GenericAlias


__all__ = ("HgTSTypeMetaData", "HgTSOutTypeMetaData",)

from hg._types._scalar_type_meta_data import HgScalarTypeMetaData
from hg._types._tsb_meta_data import HgTimeSeriesTypeMetaData


class HgTSTypeMetaData(HgTimeSeriesTypeMetaData):
    """Parses TS[...]"""

    value_scalar_tp: HgScalarTypeMetaData

    def __init__(self, scalar_type: HgScalarTypeMetaData):
        self.value_scalar_tp = scalar_type

    @property
    def is_resolved(self) -> bool:
        return self.value_scalar_tp.is_resolved

    @property
    def py_type(self) -> Type:
        from hg._types._ts_type import TS
        return TS[self.value_scalar_tp.py_type]

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"]) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return type(self)(self.value_scalar_tp.resolve(resolution_dict))

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().build_resolution_dict(resolution_dict, wired_type)
        wired_type: HgTSTypeMetaData
        self.value_scalar_tp.build_resolution_dict(resolution_dict, wired_type.value_scalar_tp if wired_type else None)

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        from hg._types._ts_type import TimeSeriesValueInput
        if isinstance(value, _GenericAlias) and value.__origin__ is TimeSeriesValueInput:
            return HgTSTypeMetaData(HgScalarTypeMetaData.parse(value.__args__[0]))

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTSTypeMetaData and self.value_scalar_tp == o.value_scalar_tp

    def __str__(self) -> str:
        return f'TS[{str(self.value_scalar_tp)}]'

    def __repr__(self) -> str:
        return f'HgTSTypeMetaData({repr(self.value_scalar_tp)})'

    def __hash__(self) -> int:
        from hg._types._ts_type import TS
        return hash(TS) ^ hash(self.value_scalar_tp)


class HgTSOutTypeMetaData(HgTSTypeMetaData):
    """Parses TSOut[...]"""

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        from hg._types._ts_type import TimeSeriesValueOutput
        if isinstance(value, _GenericAlias) and value.__origin__ is TimeSeriesValueOutput:
            return HgTSOutTypeMetaData(HgScalarTypeMetaData.parse(value.__args__[0]))

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTSOutTypeMetaData and self.value_scalar_tp == o.value_scalar_tp

    def __str__(self) -> str:
        return f'TS_OUT[{str(self.value_scalar_tp)}]'

    def __repr__(self) -> str:
        return f'HgTSOutTypeMetaData({repr(self.value_scalar_tp)})'
