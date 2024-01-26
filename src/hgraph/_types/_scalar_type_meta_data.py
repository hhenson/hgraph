from abc import abstractmethod
from collections.abc import Mapping, Set
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from types import GenericAlias
from typing import TypeVar, Type, Optional, Sequence, _GenericAlias, Callable, cast, List

import numpy as np
from frozendict import frozendict

from hgraph._types._scalar_types import Size, STATE
from hgraph._types._scalar_value import ScalarValue, Array
from hgraph._types._type_meta_data import HgTypeMetaData, ParseError

__all__ = ("HgScalarTypeMetaData", "HgTupleScalarType", "HgDictScalarType", "HgSetScalarType", "HgCollectionType",
           "HgAtomicType", "HgScalarTypeVar", "HgCompoundScalarType", "HgTupleFixedScalarType",
           "HgTupleCollectionScalarType", "HgInjectableType", "HgTypeOfTypeMetaData", "HgEvaluationClockType",
           "HgEvaluationEngineApiType", "HgStateType", "HgOutputType", "HgSchedulerType", "Injector",
           "HgArrayScalarTypeMetaData")


class HgScalarTypeMetaData(HgTypeMetaData):
    is_scalar = True

    @classmethod
    def parse(cls, value) -> "HgScalarTypeMetaData":
        parses = [HgAtomicType, HgTupleScalarType, HgDictScalarType, HgSetScalarType, HgCompoundScalarType,
                  HgScalarTypeVar, HgTypeOfTypeMetaData, HgArrayScalarTypeMetaData, HgInjectableType, HgObjectType]
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

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return tp.is_scalar and (isinstance(tp, HgScalarTypeMetaData) or isinstance(tp.py_type, self.contraints()))

    @property
    def type_var(self) -> TypeVar:
        return self.py_type

    def is_sub_class(self, tp: "HgTypeMetaData") -> bool:
        return False

    def is_convertable(self, tp: "HgTypeMetaData") -> bool:
        return False

    @property
    def operator_rank(self) -> float:
        # This is a complete wild card, so this is the weakest match (which strangely is 1.0)
        return 1.

    def constraints(self) -> Sequence[type]:
        if self.py_type.__constraints__:
            return self.py_type.__constraints__
        elif self.py_type.__bound__:
            return [self.py_type.__bound__]
        raise RuntimeError("Unexpected item in the bagging areas")

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if tp := resolution_dict.get(self.py_type):
            return tp
        elif not weak:
            raise ParseError(f"No resolution available for '{str(self)}'")
        else:
            return self

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if not wired_type.is_scalar:
            raise ParseError(f"Scalar TypeVar '{str(self)}' does not match non-scalar type: '{str(wired_type)}'")
        type_var: TypeVar = cast(TypeVar, self.py_type)
        if self == wired_type:
            return  # No additional information can be gleaned!
        if type_var in resolution_dict:
            if resolution_dict[type_var] != wired_type:
                from hgraph._wiring._wiring_errors import TemplateTypeIncompatibleResolution
                raise TemplateTypeIncompatibleResolution(self, resolution_dict[type_var], wired_type)
        else:
            resolution_dict[type_var] = wired_type

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

            from hgraph._types._time_series_types import TimeSeries
            for constraint in constraints:
                if isinstance(constraint, TimeSeries):
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

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return ((tp_ := type(tp)) is HgAtomicType and self.py_type == tp.py_type) or tp_ is HgScalarTypeVar

    def is_sub_class(self, tp: "HgTypeMetaData") -> bool:
        return issubclass(tp.py_type, self.py_type) if tp is HgAtomicType else False

    def is_convertable(self, tp: "HgTypeMetaData") -> bool:
        return tp is HgAtomicType and self.py_type in tp.convertable_types

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        if self.py_type != wired_type.py_type:
            from hgraph._wiring._wiring_errors import IncorrectTypeBinding
            raise IncorrectTypeBinding(self, wired_type)

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        value_tp = value if isinstance(value, type) else type(value)
        if issubclass(value_tp, Size):
            return HgAtomicType(value_tp, tuple())
        if issubclass(value_tp, Enum):
            return HgAtomicType(value_tp, (str, int))
        return {
            bool: lambda: HgAtomicType(bool, (int, float, str)),
            int: lambda: HgAtomicType(int, (bool, float, str)),
            float: lambda: HgAtomicType(float, (bool, int, str, datetime, timedelta)),
            date: lambda: HgAtomicType(date, (str,)),
            datetime: lambda: HgAtomicType(datetime, (float, str)),
            time: lambda: HgAtomicType(time, (str,)),
            timedelta: lambda: HgAtomicType(timedelta, (float, str,)),
            str: lambda: HgAtomicType(str, (bool, int, float, date, datetime, time)),
            ScalarValue: lambda: HgAtomicType(ScalarValue, (bool, int, float, str, date, datetime, time, timedelta)),
        }.get(value_tp, lambda: None)()


class HgObjectType(HgAtomicType):
    """A catch-all type that will allow any valid pythong type to be a scalar value"""

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return ((tp_ := type(tp)) is HgObjectType and self.py_type == tp.py_type) or (tp_ is HgScalarTypeVar and tp_.matches(self))

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        if not isinstance(value, type):
            value = type(value)
        return HgObjectType(value, tuple())

    def __repr__(self) -> str:
        return f'HgObjectType({repr(self.py_type)})'


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
        return f'{type(self).__name__}({repr(self.py_type)})'

    def __hash__(self) -> int:
        return hash(self.py_type)

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        if wired_type is not None and self.py_type != wired_type.py_type:
            from hgraph import IncorrectTypeBinding
            raise IncorrectTypeBinding(self, wired_type)

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        value_tp = value if isinstance(value, type) else type(value)
        from hgraph._runtime._evaluation_clock import EvaluationClock
        from hgraph._runtime._evaluation_engine import EvaluationEngineApi
        from hgraph._runtime._node import SCHEDULER
        return {
            EvaluationClock: lambda: HgEvaluationClockType(),
            EvaluationClockInjector: lambda: HgEvaluationClockType(),
            EvaluationEngineApi: lambda: HgEvaluationEngineApiType(),
            EvaluationEngineApiInjector: lambda: HgEvaluationEngineApiType(),
            STATE: lambda: HgStateType(),
            StateInjector: lambda: HgStateType(),
            SCHEDULER: lambda: HgSchedulerType(),
            SchedulerInjector: lambda: HgSchedulerType(),
        }.get(value_tp, lambda: None)()


class Injector:

    @abstractmethod
    def __call__(self, node):
        ...


class EvaluationClockInjector(Injector):

    def __call__(self, node):
        return node.graph.evaluation_clock


class HgEvaluationClockType(HgInjectableType):
    def __init__(self):
        from hgraph._runtime import EvaluationClock
        super().__init__(EvaluationClock)

    @property
    def injector(self):
        return EvaluationClockInjector()


class EvaluationEngineApiInjector(Injector):

    def __call__(self, node):
        return node.graph.evaluation_engine_api


class HgEvaluationEngineApiType(HgInjectableType):
    def __init__(self):
        from hgraph._runtime._evaluation_engine import EvaluationEngineApi
        super().__init__(EvaluationEngineApi)

    @property
    def injector(self):
        return EvaluationEngineApiInjector()


class StateInjector(Injector):

    def __init__(self):
        self._state = STATE()

    def __call__(self, node):
        return self._state


class HgStateType(HgInjectableType):
    def __init__(self):
        super().__init__(object)

    @property
    def injector(self):
        return StateInjector()


class OutputInjector(Injector):

    def __call__(self, node):
        return node.output


class HgOutputType(HgInjectableType):

    def __init__(self, py_type: Type = None):
        from hgraph._types._time_series_types import OUTPUT_TYPE
        super().__init__(OUTPUT_TYPE if py_type is None else py_type)

    @property
    def injector(self):
        return OutputInjector()


class SchedulerInjector(Injector):

    def __call__(self, node):
        return node.scheduler


class HgSchedulerType(HgInjectableType):

    def __init__(self):
        from hgraph._runtime._node import SCHEDULER
        super().__init__(SCHEDULER)

    @property
    def injector(self):
        return SchedulerInjector()

    def __str__(self):
        return "SCHEDULER"


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
        elif isinstance(value, tuple) and len(value) > 0:
            tp = type(value[0])
            if all(type(v) is tp for v in value):
                return HgTupleCollectionScalarType(HgScalarTypeMetaData.parse(tp))
            else:
                return HgTupleFixedScalarType(HgScalarTypeMetaData.parse(type(v)) for v in value)


class HgTupleCollectionScalarType(HgTupleScalarType):
    py_collection_type = tuple  # This is an immutable list
    element_type: HgScalarTypeMetaData  # The type items in the list

    def __init__(self, element_type: HgScalarTypeMetaData):
        self.element_type = element_type

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return type(tp) is HgTupleCollectionScalarType and self.element_type.matches(tp.element_type)

    @property
    def py_type(self) -> Type:
        return self.py_collection_type[self.element_type.py_type, ...]

    @property
    def operator_rank(self) -> float:
        return self.element_type.operator_rank / 100.

    @property
    def is_resolved(self) -> bool:
        return self.element_type.is_resolved

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return HgTupleCollectionScalarType(self.element_type.resolve(resolution_dict, weak))

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
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


class HgArrayScalarTypeMetaData(HgCollectionType):
    py_collection_type = np.ndarray
    element_type: HgScalarTypeMetaData  # The type items in the list
    shape_types: tuple[HgScalarTypeMetaData]  # Size or SIZE

    def __init__(self, element_type: HgScalarTypeMetaData, shape_types: tuple[HgScalarTypeMetaData]):
        self.element_type = element_type
        self.shape_types = shape_types

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return type(tp) is HgArrayScalarTypeMetaData and self.element_type.matches(tp.element_type) and \
            len(self.shape_types) == len(tp.shape_types) and \
            all(s1.matches(s2) for s1, s2 in zip(self.shape_types, tp.shape_types))

    @property
    def py_type(self) -> Type:
        return Array[self.element_type.py_type, *(tp.py_type for tp in self.shape_types)]

    @property
    def operator_rank(self) -> float:
        return self.element_type.operator_rank / 100.

    @property
    def is_resolved(self) -> bool:
        return self.element_type.is_resolved and all(tp.is_resolved for tp in self.shape_types)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return HgArrayScalarTypeMetaData(
                self.element_type.resolve(resolution_dict, weak),
                tuple(tp.resolve(resolution_dict, weak) for tp in self.shape_types)
            )

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        wired_type: HgArrayScalarTypeMetaData
        self.element_type.build_resolution_dict(resolution_dict, wired_type.element_type)
        for tp1, tp2 in zip(self.shape_types, wired_type.shape_types):
            tp1.element_type.build_resolution_dict(resolution_dict, tp2)

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        if isinstance(value, (GenericAlias, _GenericAlias)) and value.__origin__ == np.ndarray:
            tp = HgScalarTypeMetaData.parse(value.__args__[0])
            shape_types = tuple(HgScalarTypeMetaData.parse(v) for v in value.__args__[1:])
            if tp is None:
                raise ParseError(f"Could not parse {value.__args__[0]} as type from {value}")
            if any(tp is None for tp in shape_types):
                raise ParseError(f"Could not parse shape from {value}")
            return HgArrayScalarTypeMetaData(tp, shape_types)

    def __eq__(self, o: object) -> bool:
        return type(o) is HgArrayScalarTypeMetaData and self.element_type == o.element_type and \
            self.shape_types == o.shape_types

    def __str__(self) -> str:
        return f'Array[{str(self.element_type)}, {",".join(map(str, self.shape_types))}]'

    def __repr__(self) -> str:
        return (f'HgArrayScalarTypeMetaData({repr(self.element_type)}'
                f'{", " if self.shape_types else ""}'
                f'{", ".join(map(repr(s) for s in self.shape_types))})')

    def __hash__(self) -> int:
        return hash(tuple) ^ hash(self.element_type) ^ hash(self.shape_types)


class HgTupleFixedScalarType(HgTupleScalarType):

    def __init__(self, tp_s: Sequence[HgScalarTypeMetaData]):
        self.element_types: tuple[HgScalarTypeMetaData] = tuple(tp_s)

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return type(tp) is HgTupleFixedScalarType and len(self.element_types) == len(tp.element_types) and \
            all(e.matches(w_e) for e, w_e in zip(self.element_types, tp.element_types))

    def is_sub_class(self, tp: "HgTypeMetaData") -> bool:
        return False

    def is_convertable(self, tp: "HgTypeMetaData") -> bool:
        return False

    @property
    def operator_rank(self) -> float:
        return sum(t.operator_rank for t in self.element_types) / 100.

    @property
    def py_type(self) -> Type:
        args = tuple(tp.py_type for tp in self.element_types)
        return self.py_collection_type.__class_getitem__(args)

    @property
    def is_resolved(self) -> bool:
        return all(e.is_resolved for e in self.element_types)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            tps = tuple(tp.resolve(resolution_dict, weak) for tp in self.element_types)
            return HgTupleFixedScalarType(tps)

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
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
    py_collection_type = Set  # This is an immutable set
    element_type: HgScalarTypeMetaData  # The type items in the set

    def __init__(self, element_type: HgScalarTypeMetaData):
        self.element_type = element_type

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return type(tp) is HgSetScalarType and self.element_type.matches(tp.element_type)

    @property
    def py_type(self) -> Type:
        return self.py_collection_type[self.element_type.py_type]

    @property
    def is_resolved(self) -> bool:
        return self.element_type.is_resolved

    @property
    def operator_rank(self) -> float:
        return self.element_type.operator_rank / 100.

    @classmethod
    def parse(cls, value) -> "HgScalarTypeMetaData":
        if isinstance(value, (GenericAlias, _GenericAlias)) and value.__origin__ in [set, frozenset, Set]:
            if scalar_type := HgScalarTypeMetaData.parse(value.__args__[0]):
                return HgSetScalarType(scalar_type)
        elif isinstance(value, (set, frozenset)) and len(value) > 0:
            scalar_type = HgScalarTypeMetaData.parse(type(next(iter(value))))
            if scalar_type:
                return HgSetScalarType(scalar_type)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return HgSetScalarType(self.element_type.resolve(resolution_dict, weak))

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        self.element_type.build_resolution_dict(resolution_dict, wired_type.element_type)

    def __eq__(self, o: object) -> bool:
        return type(o) is HgSetScalarType and self.element_type == o.element_type

    def __str__(self) -> str:
        return f'Set[{str(self.element_type)}]'

    def __repr__(self) -> str:
        return f'HgSetScalarType({repr(self.element_type)})'

    def __hash__(self) -> int:
        return hash(frozenset) ^ hash(self.element_type)


class HgDictScalarType(HgCollectionType):
    py_collection_type = Mapping  # This is an immutable dict
    key_type: HgScalarTypeMetaData
    value_type: HgScalarTypeMetaData

    def __init__(self, key_type: HgScalarTypeMetaData, value_type: HgScalarTypeMetaData):
        self.key_type = key_type
        self.value_type = value_type

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return type(tp) is HgDictScalarType and self.key_type.matches(tp.key_type) and self.value_type.matches(
            tp.value_type)

    @property
    def py_type(self) -> Type:
        return self.py_collection_type[self.key_type.py_type, self.value_type.py_type]

    @property
    def is_resolved(self) -> bool:
        return self.key_type.is_resolved and self.value_type.is_resolved

    @property
    def operator_rank(self) -> float:
        return (self.key_type.operator_rank + self.value_type.operator_rank) / 100.

    @classmethod
    def parse(cls, value) -> "HgScalarTypeMetaData":
        if isinstance(value, (GenericAlias, _GenericAlias)) and value.__origin__ in [frozendict, dict, Mapping]:
            if (key_tp := HgScalarTypeMetaData.parse(value.__args__[0])) and (
                    value_tp := HgScalarTypeMetaData.parse(value.__args__[1])):
                return HgDictScalarType(key_tp, value_tp)
        elif isinstance(value, (dict, frozendict)) and len(value) > 0:
            key, value = next(iter(value.items()))
            key_tp = HgScalarTypeMetaData.parse(type(key))
            value_tp = HgScalarTypeMetaData.parse(value)
            if key_tp and value_tp:
                return HgDictScalarType(key_tp, value_tp)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return HgDictScalarType(self.key_type.resolve(resolution_dict, weak),
                                    self.value_type.resolve(resolution_dict, weak))

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        wired_type: HgDictScalarType
        self.key_type.build_resolution_dict(resolution_dict, wired_type.key_type)
        self.value_type.build_resolution_dict(resolution_dict, wired_type.value_type)

    def __eq__(self, o: object) -> bool:
        return type(o) is HgDictScalarType and self.key_type == o.key_type and self.value_type == o.value_type

    def __str__(self) -> str:
        return f'Mapping[{str(self.key_type)}, {str(self.value_type)}]'

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

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return type(tp) is HgCompoundScalarType and all(v.matches(v_tp) for v, v_tp in
                                                        zip(self.meta_data_schema.values(),
                                                            tp.meta_data_schema.values()))

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
        from hgraph._types._scalar_types import CompoundScalar
        if isinstance(value, type) and issubclass(value, CompoundScalar):
            return HgCompoundScalarType(value)
        if isinstance(value, CompoundScalar):
            return HgCompoundScalarType(type(value))

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            schema = {k: v.resolve(resolution_dict, weak) for k, v in self.meta_data_schema.items()}
            return HgCompoundScalarType(self.py_type._create_resolved_class(schema))

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
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

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return type(tp) is HgTypeOfTypeMetaData and self.value_tp.matches(tp.value_tp)

    @property
    def is_resolved(self) -> bool:
        return self.value_tp.is_resolved

    @property
    def py_type(self) -> Type:
        return type[self.value_tp.py_type]

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            resolved = self.value_tp.resolve(resolution_dict, weak)
            if not weak and not resolved.is_resolved:
                raise ParseError(f"{self} was unable to resolve after two rounds, left with: {resolved}")
            return type(self)(resolved)

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if type(wired_type) is not HgTypeOfTypeMetaData:
            raise ParseError(f"Wired type '{wired_type}' is not a type value")
        wired_type: HgTypeOfTypeMetaData
        self.value_tp.build_resolution_dict(resolution_dict, wired_type.value_tp)

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
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
