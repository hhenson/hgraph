from hashlib import sha1
from itertools import chain
from typing import Type, Optional, TypeVar, _GenericAlias, Dict

from hgraph._types._typing_utils import nth

from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData, HgDictScalarType
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._ts_type_var_meta_data import HgTsTypeVarTypeMetaData
from hgraph._types._type_meta_data import ParseError, HgTypeMetaData
from hgraph._types._scalar_types import CompoundScalar

__all__ = ("HgTimeSeriesSchemaTypeMetaData", "HgTSBTypeMetaData",)


class HgTimeSeriesSchemaTypeMetaData(HgTimeSeriesTypeMetaData):
    """
    Parses time series schema types, for example:
    ```python
    class MySchema(TimeSeriesSchema):
        p1: TS[str]
    ```
    """

    py_type: Type

    def __init__(self, py_type):
        self.py_type = py_type

    def __getitem__(self, item):
        if type(item) is int:
            return nth(self.meta_data_schema.values(), item)
        else:
            return self.meta_data_schema[item]

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return (tp_ := type(tp)) is HgTsTypeVarTypeMetaData or \
            (tp_ is HgTimeSeriesSchemaTypeMetaData and \
             self.meta_data_schema.keys() == tp.meta_data_schema.keys() and \
             all(v.matches(w_v) for v, w_v in zip(self.meta_data_schema.values(), tp.meta_data_schema.values())))

    @property
    def meta_data_schema(self) -> dict[str, "HgTimeSeriesTypeMetaData"]:
        return self.py_type.__meta_data_schema__

    @property
    def is_resolved(self) -> bool:
        return all(v.is_resolved for v in self.meta_data_schema.values())

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            schema = {k: v.resolve(resolution_dict, weak) for k, v in self.meta_data_schema.items()}
            return HgTimeSeriesSchemaTypeMetaData(self.py_type._create_resolved_class(schema))

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        wired_type: HgTimeSeriesSchemaTypeMetaData
        if len(self.meta_data_schema) != len(wired_type.meta_data_schema):
            raise ParseError(f"'{self.py_type}' schema does not match '{wired_type.py_type}'")
        if any(k not in wired_type.meta_data_schema for k in self.meta_data_schema.keys()):
            raise ParseError("Keys of schema do not match")
        for v, w_v in zip(self.meta_data_schema.values(), wired_type.meta_data_schema.values()):
            v.build_resolution_dict(resolution_dict, w_v)

    def build_resolution_dict_from_scalar(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"],
                                          wired_type: "HgTypeMetaData", value: object):
        if isinstance(wired_type, HgDictScalarType):
            value: Dict
            for k, v in self.meta_data_schema.items():
                if k in value:
                    k_value = value[k]
                    v.build_resolution_dict_from_scalar(resolution_dict, HgTypeMetaData.parse_value(k_value), k_value)

        # not sure if there are other scalar types applicable

    def scalar_type(self) -> "HgScalarTypeMetaData":
        if s := self.py_type.scalar_type():
            return HgTypeMetaData.parse_type(s)
        else:
            return HgTypeMetaData.parse_type(Dict[str, object])

    @property
    def has_references(self) -> bool:
        return any(tp.has_references for tp in self.meta_data_schema.values())

    def dereference(self) -> "HgTimeSeriesTypeMetaData":
        if self.has_references:
            schema = {k: v.dereference() for k, v in self.meta_data_schema.items()}
            from hgraph import ts_schema
            return HgTimeSeriesSchemaTypeMetaData(ts_schema(**schema))
        else:
            return self

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._tsb_type import TimeSeriesSchema
        if isinstance(value_tp, type) and issubclass(value_tp, TimeSeriesSchema) and not value_tp is TimeSeriesSchema:
            return HgTimeSeriesSchemaTypeMetaData(value_tp)
        elif isinstance(value_tp, CompoundScalar):
            return HgTimeSeriesSchemaTypeMetaData(TimeSeriesSchema.from_scalar_schema(value_tp))
        return None

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTimeSeriesSchemaTypeMetaData and \
            self.meta_data_schema == o.meta_data_schema

    def __str__(self) -> str:
        return self.py_type.__name__

    def __repr__(self) -> str:
        return f'HgTimeSeriesSchemaTypeMetaData({repr(self.py_type)})'

    def __hash__(self) -> int:
        return hash(self.meta_data_schema)


class HgTSBTypeMetaData(HgTimeSeriesTypeMetaData):
    bundle_schema_tp: HgTimeSeriesSchemaTypeMetaData

    def __init__(self, schema):
        self.bundle_schema_tp = schema

    @property
    def is_resolved(self) -> bool:
        return self.bundle_schema_tp.is_resolved

    @property
    def py_type(self) -> Type:
        from hgraph._types import TSB
        return TSB[self.bundle_schema_tp.py_type]

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return type(self)(self.bundle_schema_tp.resolve(resolution_dict, weak))

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        wired_type: HgTSBTypeMetaData
        self.bundle_schema_tp.build_resolution_dict(resolution_dict, wired_type.bundle_schema_tp)

    def build_resolution_dict_from_scalar(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"],
                                          wired_type: "HgTypeMetaData", value: object):
        self.bundle_schema_tp.build_resolution_dict_from_scalar(resolution_dict, wired_type, value)

    def scalar_type(self) -> "HgScalarTypeMetaData":
        return self.bundle_schema_tp.scalar_type()

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._tsb_type import TimeSeriesBundleInput
        if isinstance(value_tp, _GenericAlias) and value_tp.__origin__ is TimeSeriesBundleInput:
            bundle_tp = HgTimeSeriesTypeMetaData.parse_type(value_tp.__args__[0])
            if bundle_tp is None or not isinstance(bundle_tp,
                                                   (HgTimeSeriesSchemaTypeMetaData, HgTsTypeVarTypeMetaData)):
                raise ParseError(f"'{value_tp.__args__[0]}' is not a valid input to TSB")
            return HgTSBTypeMetaData(bundle_tp)

    @property
    def has_references(self) -> bool:
        return self.bundle_schema_tp.has_references

    def dereference(self) -> "HgTimeSeriesTypeMetaData":
        if self.has_references:
            return HgTSBTypeMetaData(self.bundle_schema_tp.dereference())
        else:
            return self

    @property
    def typevars(self):
        return tuple(chain(t.typevars for t in self.bundle_schema_tp.meta_data_schema.values()))

    @property
    def operator_rank(self) -> float:
        if isinstance(self.bundle_schema_tp, HgTsTypeVarTypeMetaData):
            return self.bundle_schema_tp.operator_rank
        else:
            return sum(t.operator_rank for t in self.bundle_schema_tp.meta_data_schema.values()) / 100.

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return type(tp) is HgTSBTypeMetaData and self.bundle_schema_tp.matches(tp.bundle_schema_tp)

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTSBTypeMetaData and self.bundle_schema_tp == o.bundle_schema_tp

    def __str__(self) -> str:
        return f'TSB[{str(self.bundle_schema_tp)}]'

    def __repr__(self) -> str:
        return f'HgTSBTypeMetaData({repr(self.bundle_schema_tp)})'

    def __hash__(self) -> int:
        from hgraph._types import TSB
        return hash(TSB) ^ hash(self.bundle_schema_tp)

    def __getitem__(self, item):
        return self.bundle_schema_tp[item]
