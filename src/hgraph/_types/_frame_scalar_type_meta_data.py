from types import GenericAlias
from typing import Type, TypeVar, Optional, _GenericAlias, _SpecialGenericAlias

from hgraph._types._generic_rank_util import scale_rank
from hgraph._types._scalar_type_meta_data import (
    HgCollectionType,
    HgCompoundScalarType,
    HgScalarTypeMetaData,
)
from hgraph._types._scalar_types import CompoundScalar, compound_scalar
from hgraph._types._type_meta_data import ParseError
from hgraph._types._typing_utils import class_or_instance_method

try:
    import polars as pl

    __all__ = ("SCHEMA", "Frame", "HgDataFrameScalarTypeMetaData", "Series", "HgSeriesScalarTypeMetaData")

    SCHEMA = TypeVar("SCHEMA", bound=CompoundScalar)

    class _FrameTypeclass(_SpecialGenericAlias, _root=True): ...

    Frame = _FrameTypeclass(pl.DataFrame, 1, inst=False, name="Frame")

    class HgDataFrameScalarTypeMetaData(HgCollectionType):
        schema: HgCompoundScalarType  # The schema of the frame

        def __init__(self, schema: HgCompoundScalarType):
            self.schema = schema

        def matches(self, tp: "HgTypeMetaData") -> bool:
            return type(tp) is HgDataFrameScalarTypeMetaData and self.schema.matches(tp.schema)

        @property
        def py_type(self) -> Type:
            return Frame[self.schema.py_type]

        @property
        def type_vars(self):
            return self.schema.type_vars

        @property
        def generic_rank(self) -> dict[type, float]:
            return scale_rank(self.schema.generic_rank, 0.01)

        @property
        def is_resolved(self) -> bool:
            return self.schema.is_resolved

        def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
            if self.is_resolved:
                return self
            else:
                return HgDataFrameScalarTypeMetaData(self.schema.resolve(resolution_dict, weak))

        def do_build_resolution_dict(
            self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"
        ):
            super().do_build_resolution_dict(resolution_dict, wired_type)
            wired_type: HgDataFrameScalarTypeMetaData
            self.schema.build_resolution_dict(resolution_dict, wired_type.schema)

        @classmethod
        def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
            if isinstance(value_tp, (GenericAlias, _GenericAlias)) and value_tp.__origin__ == pl.DataFrame:
                tp = HgScalarTypeMetaData.parse_type(value_tp.__args__[0])
                if tp is None:
                    raise ParseError(f"Could not parse {value_tp.__args__[0]} as type from {value_tp}")
                return HgDataFrameScalarTypeMetaData(tp)

        @class_or_instance_method
        def parse_value(self, value) -> Optional["HgTypeMetaData"]:
            if isinstance(value, pl.DataFrame):
                schema = compound_scalar(**value.schema.to_python())
                schema = HgScalarTypeMetaData.parse_type(schema)
                if my_schema := getattr(self, 'schema', None):
                    if my_schema.matches(schema):
                        return self
                return HgDataFrameScalarTypeMetaData(schema)

        def __eq__(self, o: object) -> bool:
            return type(o) is HgDataFrameScalarTypeMetaData and self.schema == o.schema

        def __str__(self) -> str:
            return f"Frame[{str(self.schema)}]"

        def __repr__(self) -> str:
            return f"HgDataFrameScalarTypeMetaData({repr(self.schema)})"

        def __hash__(self) -> int:
            return hash(tuple) ^ hash(self.schema)

    HgScalarTypeMetaData.register_parser(HgDataFrameScalarTypeMetaData)

    class _SeriesTypeclass(_SpecialGenericAlias, _root=True): ...

    Series = _SeriesTypeclass(pl.Series, 1, inst=False, name="Series")

    class HgSeriesScalarTypeMetaData(HgCollectionType):
        value_tp: HgScalarTypeMetaData

        def __init__(self, value_tp: HgScalarTypeMetaData):
            self.value_tp = value_tp

        def matches(self, tp: "HgTypeMetaData") -> bool:
            return type(tp) is HgSeriesScalarTypeMetaData and self.value_tp.matches(tp.value_tp)

        @property
        def py_type(self) -> Type:
            return Series[self.value_tp.py_type]

        @property
        def type_vars(self):
            return self.value_tp.type_vars

        @property
        def generic_rank(self) -> dict[type, float]:
            return scale_rank(self.value_tp.generic_rank, 0.01)

        @property
        def is_resolved(self) -> bool:
            return self.value_tp.is_resolved

        def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
            if self.is_resolved:
                return self
            else:
                return HgSeriesScalarTypeMetaData(self.value_tp.resolve(resolution_dict, weak))

        def do_build_resolution_dict(
            self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"
        ):
            super().do_build_resolution_dict(resolution_dict, wired_type)
            wired_type: HgSeriesScalarTypeMetaData
            self.value_tp.build_resolution_dict(resolution_dict, wired_type.value_tp)

        @classmethod
        def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
            if isinstance(value_tp, (GenericAlias, _GenericAlias)) and value_tp.__origin__ == pl.Series:
                tp = HgScalarTypeMetaData.parse_type(value_tp.__args__[0])
                if tp is None:
                    raise ParseError(f"Could not parse {value_tp.__args__[0]} as type from {value_tp}")
                return HgSeriesScalarTypeMetaData(tp)

        @classmethod
        def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
            return None  # have not learned to parse series yet

        def __eq__(self, o: object) -> bool:
            return type(o) is HgSeriesScalarTypeMetaData and self.value_tp == o.value_tp

        def __str__(self) -> str:
            return f"Series[{str(self.value_tp)}]"

        def __repr__(self) -> str:
            return f"HgSeriesScalarTypeMetaData({repr(self.value_tp)})"

        def __hash__(self) -> int:
            return hash(tuple) ^ hash(self.value_tp)

    HgScalarTypeMetaData.register_parser(HgSeriesScalarTypeMetaData)

except:
    print("Could not import polars - Frame support not enabled")
