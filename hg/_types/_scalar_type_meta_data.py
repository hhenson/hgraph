from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from types import GenericAlias

from frozendict import frozendict
from typing import TypeVar, Type, Optional, Sequence, _GenericAlias, Callable

from hg._types._scalar_types import Size
from hg._types._type_meta_data import HgTypeMetaData, ParseError


__all__ = ( "HgScalarTypeMetaData", "HgTupleScalarType", "HgDictScalarType", "HgSetScalarType", "HgCollectionType",
            "HgAtomicType", "HgScalarTypeVar", "HgCompoundScalarType", "HgTupleFixedScalarType",
            "HgTupleCollectionScalarType", "HgInjectableType", "HgTypeOfTypeMetaData")


class HgScalarTypeMetaData(HgTypeMetaData):
    is_scalar = True

    @classmethod
    def parse(cls, value) -> "HgScalarTypeMetaData":
        parses = [HgAtomicType, HgTupleScalarType, HgDictScalarType, HgSetScalarType, HgCompoundScalarType,
                  HgScalarTypeVar, HgTypeOfTypeMetaData, HgInjectableType]
        for parser in parses:
            if meta_data := parser.parse(value):
                return meta_data


class HgScalarTypeVar(HgScalarTypeMetaData):
    is_resolved = False
    is_generic = True

    def __init__(self, py_type: TypeVar):
        self.py_type = py_type

    def __eq__(self, o: object) -> bool:
        return type(o) is HgScalarTypeVar and self.py_type is o.py_type

    def __str__(self) -> str:
        return f'{self.py_type.__name__}'

    def __repr__(self) -> str:
        return f'HgScalarTypeVar({repr(self.py_type)})'

    def __hash__(self) -> int:
        return hash(self.py_type)

    @property
    def type_var(self) -> TypeVar:
        return self.py_type

    def is_sub_class(self, tp: "HgTypeMetaData") -> bool:
        return False

    def is_convertable(self, tp: "HgTypeMetaData") -> bool:
        return False

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"]) -> "HgTypeMetaData":
        if tp := resolution_dict.get(self.py_type):
            return tp
        else:
            raise ParseError(f"No resolution available for '{str(self)}'")

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if not wired_type.is_scalar:
            raise ParseError(f"Scalar TypeVar '{str(self)}' does not match non-scalar type: '{str(wired_type)}'")
        if self in resolution_dict:
            if resolution_dict[self.py_type] != wired_type:
                raise ParseError(f"TypeVar '{str(self)}' has already been resolved to"
                                 f" '{str(resolution_dict[self.py_type])}' which does not match the type "
                                 f"'{str(wired_type)}'")
        else:
            resolution_dict[self.py_type] = wired_type

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        # This is a more expensive check, we should probably cache results at some point in time.
        if isinstance(value, TypeVar):
            if value.__constraints__:
                constraints = value.__constraints__
            elif value.__bound__:
                constraints = [value.__bound__]
            else:
                return None

            from hg._types._scalar_types import _UnSet
            from hg._types._scalar_types import is_scalar
            for constraint in constraints:
                if not is_scalar(constraint) and constraint is not _UnSet:
                    return None

            return HgScalarTypeVar(value)


class HgAtomicType(HgScalarTypeMetaData):
    is_atomic = True
    is_resolved = True

    def __init__(self, py_type: Type, convertable_types: tuple[Type, ...]):
        self.py_type = py_type
        self.convertable_types = convertable_types

    def __eq__(self, o: object) -> bool:
        return type(o) is HgAtomicType and self.py_type is o.py_type

    def __str__(self) -> str:
        return f'{self.py_type.__name__}'

    def __repr__(self) -> str:
        return f'HgAtomicType({repr(self.py_type)})'

    def __hash__(self) -> int:
        return hash(self.py_type)

    def is_sub_class(self, tp: "HgTypeMetaData") -> bool:
        return issubclass(tp.py_type, self.py_type) if tp is HgAtomicType else False

    def is_convertable(self, tp: "HgTypeMetaData") -> bool:
        return tp is HgAtomicType and self.py_type in tp.convertable_types

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().build_resolution_dict(resolution_dict, wired_type)
        if self.py_type != wired_type.py_type:
            raise f"Type '{str(self)}' is not the same as the wired type '{str(wired_type)}'"

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        value_tp = value if isinstance(value, type) else type(value)
        if issubclass(value_tp, Size):
            return HgAtomicType(value_tp, tuple())
        return {
            bool: lambda: HgAtomicType(bool, (int, float, str)),
            int: lambda: HgAtomicType(int, (bool, float, str)),
            float: lambda: HgAtomicType(float, (bool, int, str, datetime, timedelta)),
            date: lambda: HgAtomicType(date, (str,)),
            datetime: lambda: HgAtomicType(datetime, (float, str)),
            time: lambda: HgAtomicType(time, (str,)),
            timedelta: lambda: HgAtomicType(timedelta, (float, str,)),
            str: lambda: HgAtomicType(str, (bool, int, float, date, datetime, time)),
        }.get(value_tp, lambda: None)()


class HgInjectableType(HgScalarTypeMetaData):
    """
    Injectable types are marker types that are used to indicate an interesting a special injectable type to be
    provided in a component signature. For example:

    ``ExecutionContext`` which injects the runtime execution context into the node call.
    """
    is_atomic = False
    is_resolved = True
    is_injectable = True

    def __init__(self, py_type: Type):
        self.py_type = py_type

    def __eq__(self, o: object) -> bool:
        return type(o) is HgInjectableType and self.py_type is o.py_type

    def __str__(self) -> str:
        return f'{self.py_type.__name__}'

    def __repr__(self) -> str:
        return f'HgSpecialAtomicType({repr(self.py_type)})'

    def __hash__(self) -> int:
        return hash(self.py_type)

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().build_resolution_dict(resolution_dict, wired_type)
        if wired_type is not None and self.py_type != wired_type.py_type:
            raise f"Type '{str(self)}' is not the same as the wired type '{str(wired_type)}'"

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        value_tp = value if isinstance(value, type) else type(value)
        from hg import ExecutionContext
        return {
            ExecutionContext: lambda: HgExecutionContextType(),
            object: lambda: HgStateType(),
        }.get(value_tp, lambda: None)()

@dataclass
class Injector:
    fn: Callable

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)


class HgExecutionContextType(HgInjectableType):
    def __init__(self):
        from hg._runtime import ExecutionContext
        super().__init__(ExecutionContext)

    @property
    def injector(self):
        return Injector(lambda node: node.graph.context)


class HgStateType(HgInjectableType):
    def __init__(self):
        super().__init__(object)

    @property
    def injector(self):
        return Injector(lambda node: object())


class HgCollectionType(HgScalarTypeMetaData):
    is_atomic = False
    py_collection_type: Type  # The raw python type of the collection for example dict, list, etc.


class HgTupleScalarType(HgCollectionType):
    py_collection_type = tuple  # This is an immutable list

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        if isinstance(value, (GenericAlias, _GenericAlias)) and value.__origin__ == tuple:
            if len(value.__args__) == 2 and value.__args__[1] is Ellipsis:
                if tp := HgScalarTypeMetaData.parse(value.__args__[0]):
                    return HgTupleCollectionScalarType(tp)
                else:
                    raise ParseError(f'Unable to parse tuple as {repr(value.__args__[0])} is not parsable')
            else:
                tp_s = []
                for arg in value.__args__:
                    if tp := HgScalarTypeMetaData.parse(arg):
                        tp_s.append(tp)
                    else:
                        raise ParseError(f"While parsing '{repr(value)}' was unable to parse '{repr(arg)}")
                return HgTupleFixedScalarType(tp_s)


class HgTupleCollectionScalarType(HgTupleScalarType):
    py_collection_type = tuple  # This is an immutable list
    element_type: HgScalarTypeMetaData  # The type items in the list

    def __init__(self, element_type: HgScalarTypeMetaData):
        self.element_type = element_type

    @property
    def py_type(self) -> Type:
        return self.py_collection_type[self.element_type.py_type, ...]

    @property
    def is_resolved(self) -> bool:
        return self.element_type.is_resolved

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"]) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return HgTupleCollectionScalarType(self.element_type.resolve(resolution_dict))

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().build_resolution_dict(resolution_dict, wired_type)
        wired_type: HgTupleCollectionScalarType
        self.element_type.build_resolution_dict(resolution_dict, wired_type.element_type)

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTupleCollectionScalarType and self.element_type == o.element_type

    def __str__(self) -> str:
        return f'tuple[{str(self.element_type)}, ...]'

    def __repr__(self) -> str:
        return f'HgTupleCollectionScalarType({repr(self.element_type)})'

    def __hash__(self) -> int:
        return hash(tuple) ^ hash(self.element_type)


class HgTupleFixedScalarType(HgTupleScalarType):

    def __init__(self, tp_s: Sequence[HgScalarTypeMetaData]):
        self.element_types: tuple[HgScalarTypeMetaData] = tuple(tp_s)

    def is_sub_class(self, tp: "HgTypeMetaData") -> bool:
        return False

    def is_convertable(self, tp: "HgTypeMetaData") -> bool:
        return False

    @property
    def py_type(self) -> Type:
        args = tuple(tp.py_type for tp in self.element_types)
        return self.py_collection_type.__class_getitem__(args)

    @property
    def is_resolved(self) -> bool:
        return all(e.is_resolved for e in self.element_types)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"]) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            tps = tuple(tp.resolve(resolution_dict) for tp in self.element_types)
            return HgTupleFixedScalarType(tps)

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().build_resolution_dict(resolution_dict, wired_type)
        if len(self.element_types) != len(wired_type.element_types):
            raise ParseError(f"tuple types do not match input type: '{str(self)}' "
                             f"not the same as the wired input: '{str(wired_type)}'")
        for tp, w_tp in zip(self.element_types, wired_type.element_types):
            # We need to recurse for type checking
            tp.build_resolution_dict(resolution_dict, w_tp)

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTupleFixedScalarType and self.element_types == o.element_types

    def __str__(self) -> str:
        return f'tuple[{", ".join(str(e) for e in self.element_types)}]'

    def __repr__(self) -> str:
        return f'HgTupleFixedScalarType({repr(self.element_types)})'

    def __hash__(self) -> int:
        return hash(tuple) ^ hash(self.element_types)


class HgSetScalarType(HgCollectionType):
    py_collection_type = frozenset  # This is an immutable set
    element_type: HgScalarTypeMetaData  # The type items in the set

    def __init__(self, element_type: HgScalarTypeMetaData):
        self.element_type = element_type

    @property
    def py_type(self) -> Type:
        return self.py_collection_type[self.element_type.py_type]

    @property
    def is_resolved(self) -> bool:
        return self.element_type.is_resolved

    @classmethod
    def parse(cls, value) -> "HgScalarTypeMetaData":
        if isinstance(value, (GenericAlias, _GenericAlias)) and value.__origin__ in [set, frozenset]:
            if scalar_type := HgScalarTypeMetaData.parse(value.__args__[0]):
                return HgSetScalarType(scalar_type)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"]) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return HgSetScalarType(self.element_type.resolve(resolution_dict))

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().build_resolution_dict(resolution_dict, wired_type)
        self.element_type.build_resolution_dict(resolution_dict, wired_type.element_type)

    def __eq__(self, o: object) -> bool:
        return type(o) is HgSetScalarType and self.element_type == o.element_type

    def __str__(self) -> str:
        return f'frozenset[{str(self.element_type)}]'

    def __repr__(self) -> str:
        return f'HgSetScalarType({repr(self.element_type)})'

    def __hash__(self) -> int:
        return hash(frozenset) ^ hash(self.element_type)


class HgDictScalarType(HgCollectionType):
    py_collection_type = frozendict  # This is an immutable dict
    key_type: HgScalarTypeMetaData
    value_type: HgScalarTypeMetaData

    def __init__(self, key_type: HgScalarTypeMetaData, value_type: HgScalarTypeMetaData):
        self.key_type = key_type
        self.value_type = value_type

    @property
    def py_type(self) -> Type:
        return self.py_collection_type[self.key_type.py_type, self.value_type.py_type]

    @property
    def is_resolved(self) -> bool:
        return self.key_type.is_resolved and self.value_type.is_resolved

    @classmethod
    def parse(cls, value) -> "HgScalarTypeMetaData":
        if isinstance(value, (GenericAlias, _GenericAlias)) and value.__origin__ in [frozendict, dict, Mapping]:
            if (key_tp := HgScalarTypeMetaData.parse(value.__args__[0])) and (
            value_tp := HgScalarTypeMetaData.parse(value.__args__[1])):
                return HgDictScalarType(key_tp, value_tp)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"]) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return HgDictScalarType(self.key_type.resolve(resolution_dict), self.value_type.resolve(resolution_dict))

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().build_resolution_dict(resolution_dict, wired_type)
        wired_type: HgDictScalarType
        self.key_type.build_resolution_dict(resolution_dict, wired_type.key_type)
        self.value_type.build_resolution_dict(resolution_dict, wired_type.value_type)

    def __eq__(self, o: object) -> bool:
            return type(o) is HgDictScalarType and self.key_type == o.key_type and self.value_type == o.value_type

    def __str__(self) -> str:
        return f'dict[{str(self.key_type)}, {str(self.value_type)}]'

    def __repr__(self) -> str:
        return f'HgDictScalarType({repr(self.key_type)}, {repr(self.value_type)})'

    def __hash__(self) -> int:
        return hash(dict) ^ hash(self.key_type) ^ hash(self.value_type)


class HgCompoundScalarType(HgScalarTypeMetaData):

    is_atomic = False  # This has the __meta_data_schema__ associated to the type with additional type information.

    @property
    def meta_data_schema(self) -> dict[str, "HgScalarTypeMetaData"]:
        return self.py_type.__meta_data_schema__

    def __init__(self, py_type: Type):
        self.py_type = py_type

    def __eq__(self, o: object) -> bool:
        return type(o) is HgCompoundScalarType and self.py_type is o.py_type

    def __str__(self) -> str:
        return f'{self.py_type.__name__}'

    def __repr__(self) -> str:
        return f'HgCompoundScalarType({repr(self.py_type)})'

    def __hash__(self) -> int:
        return hash(self.py_type)

    @property
    def is_resolved(self) -> bool:
        return all(tp.is_resolved for tp in self.meta_data_schema.values())

    def is_sub_class(self, tp: "HgTypeMetaData") -> bool:
        return isinstance(tp, HgScalarTypeMetaData) and issubclass(self.py_type, tp.py_type)

    def is_convertable(self, tp: "HgTypeMetaData") -> bool:
        # Can also look at supporting conversions from similarly schema'd TSB's later.
        return self.is_sub_class(tp)

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        from hg._types._scalar_types import CompoundScalar
        if isinstance(value, type) and issubclass(value, CompoundScalar):
            return HgCompoundScalarType(value)
        if isinstance(value, CompoundScalar):
            return HgCompoundScalarType(type(value))

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"]) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            schema = {k: v.resolve(resolution_dict) for k, v in self.meta_data_schema.items()}
            return HgCompoundScalarType(self.py_type._create_resolved_class(schema))

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().build_resolution_dict(resolution_dict, wired_type)
        wired_type: HgCompoundScalarType
        if len(self.meta_data_schema) != len(wired_type.meta_data_schema):
            raise ParseError(f"'{self.py_type}' schema does not match '{wired_type.py_type}'")
        if any(k not in wired_type.meta_data_schema for k in self.meta_data_schema.keys()):
            raise ParseError("Keys of schema do not match")
        for v, w_v in zip(self.meta_data_schema.values(), wired_type.meta_data_schema.values()):
            v.build_resolution_dict(resolution_dict, w_v)


class HgTypeOfTypeMetaData(HgTypeMetaData):
    """
    Represents a type[...]. The type represented can be of a scalar, time-series, or unknown type.
    This is however a scalar type.

    Deals with resolving type representations, for example:
    ```python
      Type[TIME_SERIES_TYPE]
      Type[SCALAR]
      Type[TS[SCALAR]]
    ```

    These are generally used to assist with type bounding or type resolution for outputs.

    For example:

    ```python

    @generator
    def const(value: SCALAR, tp: Type[TIME_SERIES_TYPE] = TS[SCALAR]) -> TIME_SERIES_TYPE:
        ...
    ```

    In this case there is no easy way to infer the output type from the inputs other than to provide it. In this case
    we can also attempt to provide a reasonable default which will allow the type system to infer the likely type.
    """

    value_tp: HgTypeMetaData
    is_scalar = True

    def __init__(self, value_tp):
        self.value_tp = value_tp

    @property
    def is_resolved(self) -> bool:
        return self.value_tp.is_resolved

    @property
    def py_type(self) -> Type:
        return type[self.value_tp.py_type]

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"]) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            resolved = self.value_tp.resolve(resolution_dict)
            if not resolved.is_resolved:
                raise ParseError(f"{self} was unable to resolve after two rounds, left with: {resolved}")
            return type(self)(resolved)

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if not type(wired_type) == HgTypeOfTypeMetaData:
            raise ParseError(f"Wired type '{wired_type}' is not a type value")
        wired_type: HgTypeOfTypeMetaData
        self.value_tp.build_resolution_dict(resolution_dict, wired_type.value_tp)

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        from hg._types._time_series_types import TimeSeries
        from hg._types._tsb_type import TimeSeriesSchema
        if isinstance(value, (_GenericAlias, GenericAlias)) and value.__origin__ in (type, Type):
            value_tp = HgTypeMetaData.parse(value.__args__[0])
            return HgTypeOfTypeMetaData(value_tp)
        return None

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTypeOfTypeMetaData and self.value_tp == o.value_tp

    def __str__(self) -> str:
        return self.py_type.__name__

    def __repr__(self) -> str:
        return f'HgTypeOfTypeMetaData({repr(self.value_tp)})'

    def __hash__(self) -> int:
        return hash(self.py_type)
