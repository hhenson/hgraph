import itertools
import logging
import types
import typing
from abc import abstractmethod
from collections.abc import Mapping, Set
from dataclasses import MISSING as DATACLASS_MISSING, fields, is_dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from functools import partial
from statistics import fmean
from types import GenericAlias
from typing import TypeVar, Type, Optional, Sequence, _GenericAlias, cast, List, TYPE_CHECKING, get_args, get_origin

import numpy as np
from frozendict import frozendict

from hgraph._types._schema_type import AbstractSchema, SchemaRecurseContext
from hgraph._types._typing_utils import class_or_instance_method
from hgraph._types._scalar_types import WindowSize
from hgraph._types._generic_rank_util import scale_rank, combine_ranks
from hgraph._types._recordable_state import RECORDABLE_STATE
from hgraph._types._scalar_types import Size, STATE, CompoundScalar, LOGGER, UnNamedCompoundScalar
from hgraph._types._scalar_value import ScalarValue, Array
from hgraph._types._type_meta_data import HgTypeMetaData, ParseError, cpp_type_property

if TYPE_CHECKING:
    from hgraph._types._tsb_meta_data import HgTimeSeriesSchemaTypeMetaData, HgTSBTypeMetaData

__all__ = (
    "HgScalarTypeMetaData",
    "HgTupleScalarType",
    "HgDictScalarType",
    "HgSetScalarType",
    "HgCollectionType",
    "HgAtomicType",
    "HgScalarTypeVar",
    "HgCompoundScalarType",
    "HgTupleFixedScalarType",
    "HgTupleCollectionScalarType",
    "HgInjectableType",
    "HgTypeOfTypeMetaData",
    "HgEvaluationClockType",
    "HgEvaluationEngineApiType",
    "HgStateType",
    "HgRecordableStateType",
    "HgOutputType",
    "HgSchedulerType",
    "Injector",
    "RecordableStateInjector",
    "HgArrayScalarTypeMetaData",
    "HgLoggerType",
    "HgNodeType",
    "HgTraitsType",
    "HgGlobalStateType",
    "GlobalStateInjector",
)


class HgScalarTypeMetaData(HgTypeMetaData):
    is_scalar = True

    @classmethod
    def parse_type(cls, value_tp) -> "HgScalarTypeMetaData":
        for parser in cls._parsers():
            if isinstance(value_tp, parser):
                return value_tp
            if meta_data := parser.parse_type(value_tp):
                return meta_data

    @classmethod
    def parse_value(cls, value) -> "HgScalarTypeMetaData":
        for parser in cls._parsers():
            if meta_data := parser.parse_value(value):
                return meta_data

    @classmethod
    def _parsers(cls) -> List[type]:
        if p := getattr(cls, "_parsers_list", None):
            return p

        from hgraph._types._cpp_native_meta_data import HgCppNativeScalarType

        cls._parsers_list = [
            HgAtomicType,
            HgTupleScalarType,
            HgDictScalarType,
            HgSetScalarType,
            HgCppNativeScalarType,  # Must be before HgCompoundScalarType
            HgCompoundScalarType,
            HgScalarTypeVar,
            HgTypeOfTypeMetaData,
            HgArrayScalarTypeMetaData,
            HgInjectableType,
            HgStateType,
            HgRecordableStateType,
            HgObjectType,
        ]
        return cls._parsers_list

    @classmethod
    def register_parser(cls, new_parser: type):
        cls._parsers().insert(-1, new_parser)  # Insert before the HgObjectType which is catch-all


_DATACLASS_SCALAR_SCHEMA_CACHE = {}
_DATACLASS_ATTRIBUTE_MISSING = object()


def _dataclass_scalar_origin(tp):
    origin = get_origin(tp) or tp
    return (
        origin if isinstance(origin, type) and is_dataclass(origin) and not issubclass(origin, AbstractSchema) else None
    )


def _is_dataclass_scalar_type(tp) -> bool:
    return _dataclass_scalar_origin(tp) is not None


def _set_dataclass_hgraph_attributes(origin, attributes):
    for name, value in attributes.items():
        existing = vars(origin).get(name, _DATACLASS_ATTRIBUTE_MISSING)
        if existing is not _DATACLASS_ATTRIBUTE_MISSING and existing != value:
            raise ParseError(
                f"Dataclass '{origin.__qualname__}' defines reserved hgraph attribute '{name}' "
                "with an incompatible value"
            )
    for name, value in attributes.items():
        setattr(origin, name, value)


def _substitute_type_vars(tp, substitutions):
    if isinstance(tp, list):
        return [_substitute_type_vars(arg, substitutions) for arg in tp]
    try:
        if tp in substitutions:
            return substitutions[tp]
    except TypeError:
        pass

    origin = get_origin(tp)
    if origin is None:
        return tp

    args = tuple(_substitute_type_vars(arg, substitutions) for arg in get_args(tp))
    if hasattr(tp, "copy_with"):
        return tp.copy_with(args)
    if origin in (typing.Union, types.UnionType):
        return typing.Union[args]
    try:
        return origin[args[0] if len(args) == 1 else args]
    except TypeError:
        return tp


def _dataclass_scalar_schema(tp):
    if tp in _DATACLASS_SCALAR_SCHEMA_CACHE:
        return _DATACLASS_SCALAR_SCHEMA_CACHE[tp]

    origin = _dataclass_scalar_origin(tp)
    if origin is None:
        raise ParseError(f"'{tp}' is not a dataclass scalar type")

    parameters = tuple(getattr(origin, "__parameters__", ()))
    substitutions = dict(zip(parameters, get_args(tp)))
    try:
        resolved_annotations = typing.get_type_hints(origin)
    except (NameError, TypeError):
        resolved_annotations = {}

    schema = {}
    for field in fields(origin):
        annotation = _substitute_type_vars(
            resolved_annotations.get(field.name, field.type),
            substitutions,
        )
        if isinstance(annotation, (str, typing.ForwardRef)):
            raise ParseError(
                f"When parsing dataclass '{origin.__qualname__}', field '{field.name}' "
                f"has unresolved annotation {annotation!r}"
            )

        try:
            field_type = HgTypeMetaData.parse_type(annotation)
        except ParseError as error:
            raise ParseError(
                f"When parsing dataclass '{origin.__qualname__}', unable to parse field "
                f"'{field.name}' with value {annotation}"
            ) from error
        if not isinstance(field_type, HgScalarTypeMetaData):
            raise ParseError(
                f"When parsing dataclass '{origin.__qualname__}', field '{field.name}' "
                f"must be a scalar type, not {annotation}"
            )
        schema[field.name] = field_type

    schema = frozendict(schema)
    constructor_fields = tuple(field.name for field in fields(origin) if field.init)
    required_fields = tuple(
        field.name
        for field in fields(origin)
        if field.init and field.default is DATACLASS_MISSING and field.default_factory is DATACLASS_MISSING
    )

    # A few integrations inspect concrete scalar classes directly. Keep those
    # paths working without replacing or wrapping the user's class, and reject
    # incompatible user-defined values instead of silently overwriting them.
    attributes = {
        "__hgraph_bundle_constructor_fields__": constructor_fields,
        "__hgraph_bundle_required_fields__": required_fields,
    }
    if tp is origin:
        attributes["__meta_data_schema__"] = schema

    _set_dataclass_hgraph_attributes(origin, attributes)
    _DATACLASS_SCALAR_SCHEMA_CACHE[tp] = schema
    return schema


def _dataclass_scalar_is_instance(value) -> bool:
    return not isinstance(value, type) and is_dataclass(value) and _is_dataclass_scalar_type(type(value))


def _structured_scalar_origin_and_args(tp):
    return get_origin(tp) or tp, get_args(tp)


def _inherits_dataclass_specialization(candidate, target_origin, target_args):
    candidate_origin = get_origin(candidate) or candidate
    if candidate_origin is target_origin:
        return get_args(candidate) == target_args
    if not isinstance(candidate_origin, type):
        return False

    for base in getattr(candidate_origin, "__orig_bases__", ()):
        base_origin = get_origin(base) or base
        if base_origin is target_origin and get_args(base) == target_args:
            return True
        if _is_dataclass_scalar_type(base_origin) and _inherits_dataclass_specialization(
            base, target_origin, target_args
        ):
            return True
    return False


class HgScalarTypeVar(HgScalarTypeMetaData):
    is_resolved = False
    is_generic = True

    def __init__(self, py_type: TypeVar):
        self.py_type = py_type

    def __eq__(self, o: object) -> bool:
        return type(o) is HgScalarTypeVar and self.py_type is o.py_type

    def __str__(self) -> str:
        return f"{self.py_type.__name__}"

    def __repr__(self) -> str:
        return f"HgScalarTypeVar({repr(self.py_type)})"

    def __hash__(self) -> int:
        return hash(self.py_type)

    def matches(self, tp: "HgTypeMetaData") -> bool:
        if isinstance(tp, HgScalarTypeVar):
            if self.py_type == tp.py_type:
                return True
            for s_i, tp_i in itertools.product(self.constraints(), tp.constraints()):
                s_t = isinstance(s_i, HgScalarTypeMetaData)
                tp_t = isinstance(tp_i, HgScalarTypeMetaData)
                if s_t and tp_t:
                    if s_i.matches(tp_i):
                        return True
                if not s_t and tp_t:
                    if issubclass(getattr(tp.py_type, "__origin__", tp.py_type), s_i):
                        return True
                if not s_t and not tp_t:
                    if issubclass(tp_i, s_i):
                        return True
            return False

        if tp.is_scalar:
            # Get effective py_type for matching - unwrap CppNative wrappers
            from hgraph._types._cpp_native_meta_data import HgCppNativeScalarType

            effective_py_type = tp.wrapped_type.py_type if isinstance(tp, HgCppNativeScalarType) else tp.py_type

            for c in self.constraints():
                if isinstance(c, HgScalarTypeMetaData) and c.matches(tp):
                    return True
                elif c is CompoundScalar and isinstance(tp, HgCompoundScalarType):
                    return True
                else:
                    if issubclass(getattr(effective_py_type, "__origin__", effective_py_type), c):
                        return True

        return False

    @property
    def type_var(self) -> TypeVar:
        return self.py_type

    @property
    def type_vars(self):
        return {self.py_type}

    @property
    def generic_rank(self) -> dict[type, float]:
        avg_constraints_rank = fmean(
            itertools.chain(*(
                (
                    c.generic_rank.values()
                    if isinstance(c, HgTypeMetaData)
                    else [1.0 / ((c.__mro__.index(object) if hasattr(c, "__mro__") else 1) + 1.0)]
                )
                for c in self.constraints()
            ))
        )
        return {self.py_type: 0.9 + avg_constraints_rank / 10.0}

    def constraints(self) -> Sequence[type]:
        if self.py_type.__constraints__:
            return tuple(
                c if type(c) is type else HgScalarTypeMetaData.parse_type(c) for c in self.py_type.__constraints__
            )
        elif self.py_type.__bound__:
            return (self.py_type.__bound__,)
        return (object,)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if tp := resolution_dict.get(self.py_type):
            return tp
        elif not weak:
            raise ParseError(f"No resolution available for '{str(self)}'")
        else:
            return self

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if wired_type and not wired_type.is_scalar:
            raise ParseError(f"Scalar TypeVar '{str(self)}' does not match non-scalar type: '{str(wired_type)}'")
        type_var: TypeVar = cast(TypeVar, self.py_type)
        if self == wired_type:
            return  # No additional information can be gleaned!
        if type_var in resolution_dict:
            if wired_type and not resolution_dict[type_var].matches(wired_type):
                from hgraph._wiring._wiring_errors import TemplateTypeIncompatibleResolution

                raise TemplateTypeIncompatibleResolution(self, resolution_dict[type_var], wired_type)
        else:
            resolution_dict[type_var] = wired_type

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        # This is a more expensive check, we should probably cache results at some point in time.
        if isinstance(value_tp, TypeVar):
            if value_tp.__constraints__:
                constraints = value_tp.__constraints__
            elif value_tp.__bound__:
                constraints = (value_tp.__bound__,)
            else:
                constraints = ()

            from hgraph._types._time_series_types import TimeSeries

            for constraint in constraints:
                if isinstance(constraint, TimeSeries):
                    return None

            return HgScalarTypeVar(value_tp)

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        return None  # TypeVars have no values


class HgAtomicType(HgScalarTypeMetaData):
    is_atomic = True
    is_resolved = True

    def __init__(self, py_type: Type):
        self.py_type = py_type

    def __eq__(self, o: object) -> bool:
        return type(o) is HgAtomicType and self.py_type is o.py_type

    def __str__(self) -> str:
        return f"{self.py_type.__name__}"

    def __repr__(self) -> str:
        return f"HgAtomicType({repr(self.py_type)})"

    def __hash__(self) -> int:
        return hash(self.py_type)

    @property
    def generic_rank(self) -> dict[type, float]:
        return {self.py_type: 1e-10}

    @cpp_type_property
    def cpp_type(self, _hgraph):
        return _hgraph.value.get_scalar_type_meta(self.py_type)

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return ((tp_ := type(tp)) is HgAtomicType and self.py_type == tp.py_type) or tp_ is HgScalarTypeVar

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        if not issubclass(wired_type.py_type, self.py_type):
            from hgraph._wiring._wiring_errors import IncorrectTypeBinding

            raise IncorrectTypeBinding(self, wired_type)

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        if isinstance(value_tp, type):
            if issubclass(value_tp, Size):
                return HgAtomicType(value_tp)
            if issubclass(value_tp, WindowSize):
                return HgAtomicType(value_tp)
            if issubclass(value_tp, Enum):
                return HgAtomicType(value_tp)
            return {
                bool: lambda: HgAtomicType(bool),
                int: lambda: HgAtomicType(int),
                float: lambda: HgAtomicType(float),
                date: lambda: HgAtomicType(date),
                datetime: lambda: HgAtomicType(datetime),
                time: lambda: HgAtomicType(time),
                timedelta: lambda: HgAtomicType(timedelta),
                bytes: lambda: HgAtomicType(bytes),
                str: lambda: HgAtomicType(str),
                ScalarValue: lambda: HgAtomicType(ScalarValue),
            }.get(value_tp, lambda: None)()

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        if isinstance(value, type) and issubclass(value, Size):
            return HgAtomicType(value)
        if isinstance(value, type) and issubclass(value, WindowSize):
            return HgAtomicType(value)
        return HgAtomicType.parse_type(type(value))


class HgObjectType(HgAtomicType):
    """A catch-all type that will allow any valid pythong type to be a scalar value"""

    def __eq__(self, o: object) -> bool:
        return type(o) is HgObjectType and self.py_type is o.py_type

    def __hash__(self) -> int:
        return super().__hash__()

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return (
            issubclass(getattr(tp.py_type, "__origin__", tp.py_type), getattr(self.py_type, "__origin__", self.py_type))
        ) or (type(tp) is HgScalarTypeVar and tp.matches(self))

    @classmethod
    def parse_type(cls, tp) -> Optional["HgTypeMetaData"]:
        return HgObjectType(tp)

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        return HgObjectType.parse_type(type(value))

    def __repr__(self) -> str:
        return f"HgObjectType({repr(self.py_type)})"


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
        return f"{self.py_type.__name__}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}({repr(self.py_type)})"

    def __hash__(self) -> int:
        return hash(self.py_type)

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        if wired_type is not None and self.py_type != wired_type.py_type:
            from hgraph import IncorrectTypeBinding

            raise IncorrectTypeBinding(self, wired_type)

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._runtime._evaluation_clock import EvaluationClock
        from hgraph._runtime._evaluation_engine import EvaluationEngineApi
        from hgraph._runtime._global_state import GlobalState
        from hgraph._runtime._node import SCHEDULER, NODE
        from hgraph._runtime._traits import Traits

        injectables = {
            EvaluationClock: lambda: HgEvaluationClockType(),
            EvaluationClockInjector: lambda: HgEvaluationClockType(),
            EvaluationEngineApi: lambda: HgEvaluationEngineApiType(),
            EvaluationEngineApiInjector: lambda: HgEvaluationEngineApiType(),
            SCHEDULER: lambda: HgSchedulerType(),
            SchedulerInjector: lambda: HgSchedulerType(),
            LOGGER: lambda: HgLoggerType(),
            NODE: lambda: HgNodeType(),
            Traits: lambda: HgTraitsType(),
            GlobalState: lambda: HgGlobalStateType(),
            GlobalStateInjector: lambda: HgGlobalStateType(),
        }
        try:
            factory = injectables.get(value_tp)
        except TypeError:
            # Some valid typing annotations, notably Callable[[...], ...],
            # contain a list of arguments and are therefore unhashable.
            return None
        return factory() if factory else None

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        return HgInjectableType.parse_type(type(value))


class Injector:

    @abstractmethod
    def __call__(self, node): ...


class EvaluationClockInjector(Injector):

    def __call__(self, node):
        return node.graph.evaluation_clock


class TraitsInjector(Injector):

    def __call__(self, node):
        return node.graph.traits


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


class HgTraitsType(HgInjectableType):

    def __init__(self):
        from hgraph._runtime._traits import Traits

        super().__init__(Traits)

    @property
    def injector(self):
        return TraitsInjector()


class GlobalStateInjector(Injector):
    def __call__(self, node):
        from hgraph._runtime._global_state import GlobalState

        return GlobalState.instance()


class HgGlobalStateType(HgInjectableType):
    """
    Injectable for the current GlobalState instance.
    """

    def __init__(self):
        from hgraph._runtime._global_state import GlobalState

        super().__init__(GlobalState)

    @property
    def injector(self):
        return GlobalStateInjector()


class LoggerInjector(Injector):
    def __call__(self, node):
        from hgraph import GlobalState
        from hgraph._types._error_type import BackTrace

        root_logger = GlobalState.instance()["__graph_logger__"]
        custom_formatter = GlobalState.instance().get("__graph_custom_formatter__", None)
        node_path = BackTrace.runtime_path_name(node).replace(".log_impl._log", "")
        logger = root_logger.getChild(node_path)
        if custom_formatter is not None:
            logger._log = partial(custom_formatter, node_path=node_path, __orig_log__=logger._log)

        return logger


class HgLoggerType(HgInjectableType):
    """
    Injectable for logger object.
    """

    def __init__(self):
        super().__init__(logging.Logger)

    @property
    def injector(self):
        return LoggerInjector()


class NodeInjector(Injector):
    def __call__(self, node):
        return node


class HgNodeType(HgInjectableType):
    """
    Injectable for node object.
    """

    def __init__(self):
        from hgraph._runtime._node import Node

        super().__init__(Node)

    @property
    def injector(self):
        return NodeInjector()


class StateInjector(Injector):

    def __init__(self, schema):
        self.schema = schema

    def __call__(self, node):
        return STATE(__schema__=self.schema)


class HgStateType(HgInjectableType):
    """
    State can contain any valid scalar as its value. If no value is provided, then the injected
    scalar will be a dictionary.
    """

    state_type: HgScalarTypeMetaData

    def __init__(self, state_type: HgScalarTypeMetaData):
        super().__init__(STATE)
        self.state_type = state_type

    @property
    def injector(self):
        return StateInjector(self.state_type.py_type if self.state_type is not None else None)

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._scalar_types import STATE

        if isinstance(value_tp, _GenericAlias) and value_tp.__origin__ is STATE:
            bundle_tp = HgScalarTypeMetaData.parse_type(value_tp.__args__[0])
            if bundle_tp is None:
                raise ParseError(f"'{value_tp.__args__[0]}' is not a valid input to STATE")
            return HgStateType(bundle_tp)
        if value_tp is STATE:
            return HgStateType(None)

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        return cls.parse_type(type(value))

    @property
    def is_resolved(self) -> bool:
        return self.state_type is None or self.state_type.is_resolved

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        return HgStateType(self.state_type.resolve(resolution_dict, weak))


class RecordableStateInjector(Injector):

    def __init__(self, tsb_type):
        self.tsb_type: HgTSBTypeMetaData = tsb_type

    def __call__(self, node):
        return RECORDABLE_STATE(__schema__=self.tsb_type.bundle_schema_tp.py_type, **node.recordable_state)


class HgRecordableStateType(HgInjectableType):
    """
    RecordableState is similar to a TSB and is represented as a special output
    value associated to ta node. The value is made available to the node instance
    and when inside-of a recordable ``component`` this is attached to a recorder.
    It is also re-constituted when re-started in the appropriate mode.
    """

    state_type: "HgTimeSeriesSchemaTypeMetaData"

    def __init__(self, state_type: "HgTimeSeriesSchemaTypeMetaData"):
        super().__init__(STATE)
        self.state_type = state_type

    @property
    def tsb_type(self) -> "HgTSBTypeMetaData":
        from hgraph._types._tsb_meta_data import HgTSBTypeMetaData

        return HgTSBTypeMetaData(self.state_type) if self.state_type is not None else None

    @property
    def injector(self):
        return RecordableStateInjector(self.tsb_type)

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._recordable_state import RECORDABLE_STATE
        from hgraph._types._tsb_meta_data import HgTimeSeriesSchemaTypeMetaData

        if isinstance(value_tp, _GenericAlias) and value_tp.__origin__ is RECORDABLE_STATE:
            bundle_tp = HgTimeSeriesSchemaTypeMetaData.parse_type(value_tp.__args__[0])
            if bundle_tp is None:
                raise ParseError(f"'{value_tp.__args__[0]}' is not a valid input to RECORDABLE_STATE")
            return HgRecordableStateType(bundle_tp)
        if value_tp is RECORDABLE_STATE:
            raise ParseError("RECORDABLE_STATE must be provided a schema to define the structure")

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        return cls.parse_type(type(value))

    @property
    def is_resolved(self) -> bool:
        return self.state_type is None or self.state_type.is_resolved

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        tp = self.state_type.resolve(resolution_dict, weak)
        from hgraph._types._tsb_meta_data import HgTimeSeriesSchemaTypeMetaData

        if isinstance(tp, HgTimeSeriesSchemaTypeMetaData):
            return HgRecordableStateType(tp)
        else:
            from hgraph import CustomMessageWiringError

            raise CustomMessageWiringError(
                f"'{tp}' is not a valid resolution, need to be HgTimeSeriesSchemaTypeMetaData"
            )


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
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        if isinstance(value_tp, (GenericAlias, _GenericAlias)) and value_tp.__origin__ == tuple:
            if len(value_tp.__args__) == 2 and value_tp.__args__[1] is Ellipsis:
                if tp := HgScalarTypeMetaData.parse_type(value_tp.__args__[0]):
                    return HgTupleCollectionScalarType(tp)
                else:
                    raise ParseError(f"Unable to parse tuple as {repr(value_tp.__args__[0])} is not parsable")
            else:
                tp_s = []
                for arg in value_tp.__args__:
                    if tp := HgScalarTypeMetaData.parse_type(arg):
                        tp_s.append(tp)
                    else:
                        raise ParseError(f"While parsing '{repr(value_tp)}' was unable to parse '{repr(arg)}")
                return HgTupleFixedScalarType(tp_s)

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        if isinstance(value, tuple) and len(value) > 0:
            tp = type(value[0])
            if all(type(v) is tp for v in value):
                return HgTupleCollectionScalarType(HgScalarTypeMetaData.parse_value(value[0]))
            else:
                return HgTupleFixedScalarType(HgScalarTypeMetaData.parse_value(v) for v in value)


class HgTupleCollectionScalarType(HgTupleScalarType):
    py_collection_type = tuple  # This is an immutable list
    element_type: HgScalarTypeMetaData  # The type items in the list

    def __init__(self, element_type: HgScalarTypeMetaData):
        self.element_type = element_type

    @cpp_type_property
    def cpp_type(self, _hgraph):
        v = self.element_type.cpp_type
        return v and _hgraph.value.get_dynamic_list_type_meta(v)

    def matches(self, tp: "HgTypeMetaData") -> bool:
        tp_ = type(tp)
        if tp_ is HgTupleCollectionScalarType:
            return self.element_type.matches(tp.element_type)
        elif tp_ is HgTupleFixedScalarType:
            matches = all(self.element_type == tp_ for tp_ in tp.element_types) or (
                all(self.element_type.matches(tp_) for tp_ in tp.element_types)
                and all(tp_.py_type != object for tp_ in tp.element_types)
            )
            if matches and self.element_type.is_generic:
                resolution = {}
                self.element_type.build_resolution_dict(resolution, tp.element_types[0])
                resolved = self.element_type.resolve(resolution)
                return all(resolved.matches(tp_) for tp_ in tp.element_types)
            else:
                return matches
        elif tp_ is HgDictScalarType:
            # Support matching a delta value as well.
            return self.element_type.matches(tp.value_type) and tp.key_type.py_type is int
        else:
            return False

    @property
    def py_type(self) -> Type:
        return self.py_collection_type[self.element_type.py_type, ...]

    @property
    def type_vars(self):
        return self.element_type.type_vars

    @property
    def generic_rank(self) -> dict[type, float]:
        return scale_rank(self.element_type.generic_rank, 0.01)

    @property
    def is_resolved(self) -> bool:
        return self.element_type.is_resolved

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return HgTupleCollectionScalarType(self.element_type.resolve(resolution_dict, weak))

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        tp_ = type(wired_type)
        if tp_ is HgTupleCollectionScalarType:
            wired_type: HgTupleCollectionScalarType
            self.element_type.build_resolution_dict(resolution_dict, wired_type.element_type)
        elif tp_ is HgTupleFixedScalarType:
            wired_type: HgTupleFixedScalarType
            if all(self.element_type.matches(tp_) for tp_ in wired_type.element_types):
                self.element_type.build_resolution_dict(resolution_dict, wired_type.element_types[0])
        else:
            super().do_build_resolution_dict(resolution_dict, wired_type)

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTupleCollectionScalarType and self.element_type == o.element_type

    def __str__(self) -> str:
        return f"tuple[{str(self.element_type)}, ...]"

    def __repr__(self) -> str:
        return f"HgTupleCollectionScalarType({repr(self.element_type)})"

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
        return (
            type(tp) is HgArrayScalarTypeMetaData
            and self.element_type.matches(tp.element_type)
            and len(self.shape_types) == len(tp.shape_types)
            and all(s1.matches(s2) for s1, s2 in zip(self.shape_types, tp.shape_types))
        )

    @property
    def py_type(self) -> Type:
        return Array[self.element_type.py_type, *(tp.py_type for tp in self.shape_types)]

    @property
    def type_vars(self):
        return self.element_type.type_vars

    @property
    def generic_rank(self) -> dict[type, float]:
        return scale_rank(self.element_type.generic_rank, 0.01)

    @property
    def is_resolved(self) -> bool:
        return self.element_type.is_resolved and all(tp.is_resolved for tp in self.shape_types)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return HgArrayScalarTypeMetaData(
                self.element_type.resolve(resolution_dict, weak),
                tuple(tp.resolve(resolution_dict, weak) for tp in self.shape_types),
            )

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        wired_type: HgArrayScalarTypeMetaData
        self.element_type.build_resolution_dict(resolution_dict, wired_type.element_type)
        for tp1, tp2 in zip(self.shape_types, wired_type.shape_types):
            tp1.build_resolution_dict(resolution_dict, tp2)

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        if isinstance(value_tp, (GenericAlias, _GenericAlias)) and value_tp.__origin__ == np.ndarray:
            tp = HgScalarTypeMetaData.parse_type(value_tp.__args__[0])
            shape_types = tuple(HgScalarTypeMetaData.parse_type(v) for v in value_tp.__args__[1:])
            if tp is None:
                raise ParseError(f"Could not parse {value_tp.__args__[0]} as type from {value_tp}")
            if any(tp is None for tp in shape_types):
                raise ParseError(f"Could not parse shape from {value_tp}")
            return HgArrayScalarTypeMetaData(tp, shape_types)

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        return None  # have not learned to parse ndarrays yet

    def __eq__(self, o: object) -> bool:
        return (
            type(o) is HgArrayScalarTypeMetaData
            and self.element_type == o.element_type
            and self.shape_types == o.shape_types
        )

    def __str__(self) -> str:
        return f'Array[{str(self.element_type)}, {",".join(map(str, self.shape_types))}]'

    def __repr__(self) -> str:
        return (
            f"HgArrayScalarTypeMetaData({repr(self.element_type)}"
            f"{', ' if self.shape_types else ''}"
            f"{', '.join(map(repr(s) for s in self.shape_types))})"
        )

    def __hash__(self) -> int:
        return hash(tuple) ^ hash(self.element_type) ^ hash(self.shape_types)


class HgTupleFixedScalarType(HgTupleScalarType):

    def __init__(self, tp_s: Sequence[HgScalarTypeMetaData]):
        self.element_types: tuple[HgScalarTypeMetaData] = tuple(tp_s)

    def size(self) -> int:
        return len(self.element_types)

    @cpp_type_property
    def cpp_type(self, _hgraph):
        element_types = []
        for elem_type in self.element_types:
            elem_cpp = elem_type.cpp_type
            if elem_cpp is None:
                return None
            element_types.append(elem_cpp)
        return _hgraph.value.get_tuple_type_meta(element_types)

    def matches(self, tp: "HgTypeMetaData") -> bool:
        if isinstance(tp, HgTupleCollectionScalarType):
            # There is already logic in the HgTupleCollectionScalarType
            # to handle fixed type matching
            return tp.matches(self)
        return (
            type(tp) is HgTupleFixedScalarType
            and len(self.element_types) == len(tp.element_types)
            and all(e.matches(w_e) for e, w_e in zip(self.element_types, tp.element_types))
        )

    @property
    def type_vars(self):
        return set().union(*(t.type_vars for t in self.element_types))

    @property
    def generic_rank(self) -> dict[type, float]:
        return combine_ranks((e.generic_rank for e in self.element_types), 0.01)

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
            raise ParseError(
                f"tuple types do not match input type: '{str(self)}' "
                f"not the same as the wired input: '{str(wired_type)}'"
            )
        for tp, w_tp in zip(self.element_types, wired_type.element_types):
            # We need to recurse for type checking
            tp.build_resolution_dict(resolution_dict, w_tp)

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTupleFixedScalarType and self.element_types == o.element_types

    def __str__(self) -> str:
        return f'tuple[{", ".join(str(e) for e in self.element_types)}]'

    def __repr__(self) -> str:
        return f"HgTupleFixedScalarType({repr(self.element_types)})"

    def __hash__(self) -> int:
        return hash(tuple) ^ hash(self.element_types)


class HgSetScalarType(HgCollectionType):
    py_collection_type = Set  # This is an immutable set
    element_type: HgScalarTypeMetaData  # The type items in the set

    def __init__(self, element_type: HgScalarTypeMetaData):
        self.element_type = element_type

    @cpp_type_property
    def cpp_type(self, _hgraph):
        v = self.element_type.cpp_type
        return v and _hgraph.value.get_set_type_meta(v)

    def matches(self, tp: "HgTypeMetaData") -> bool:
        if (t := type(tp)) is HgSetScalarType and self.element_type.matches(tp.element_type):
            return True
        else:
            return t is HgObjectType and tp.py_type == frozenset  # accept empty sets

    @property
    def py_type(self) -> Type:
        return self.py_collection_type[self.element_type.py_type]

    @property
    def is_resolved(self) -> bool:
        return self.element_type.is_resolved

    @property
    def type_vars(self):
        return self.element_type.type_vars

    @property
    def generic_rank(self) -> dict[type, float]:
        return scale_rank(self.element_type.generic_rank, 0.01)

    @classmethod
    def parse_type(cls, value_tp) -> "HgScalarTypeMetaData":
        if isinstance(value_tp, (GenericAlias, _GenericAlias)) and value_tp.__origin__ in [set, frozenset, Set]:
            if scalar_type := HgScalarTypeMetaData.parse_type(value_tp.__args__[0]):
                return HgSetScalarType(scalar_type)

    @classmethod
    def parse_value(cls, value) -> "HgScalarTypeMetaData":
        if isinstance(value, (set, frozenset)) and len(value) > 0:
            scalar_type = HgScalarTypeMetaData.parse_value(next(iter(value)))
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
        return f"Set[{str(self.element_type)}]"

    def __repr__(self) -> str:
        return f"HgSetScalarType({repr(self.element_type)})"

    def __hash__(self) -> int:
        return hash(frozenset) ^ hash(self.element_type)


class HgDictScalarType(HgCollectionType):
    py_collection_type = Mapping  # This is an immutable dict
    key_type: HgScalarTypeMetaData
    value_type: HgScalarTypeMetaData

    def __init__(self, key_type: HgScalarTypeMetaData, value_type: HgScalarTypeMetaData):
        self.key_type = key_type
        self.value_type = value_type

    @cpp_type_property
    def cpp_type(self, _hgraph):
        key_cpp = self.key_type.cpp_type
        value_cpp = self.value_type.cpp_type
        if key_cpp is None or value_cpp is None:
            return None
        return _hgraph.value.get_dict_type_meta(key_cpp, value_cpp)

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return (
            type(tp) is HgDictScalarType
            and self.key_type.matches(tp.key_type)
            and self.value_type.matches(tp.value_type)
        ) or (
            type(tp) is HgObjectType and tp.py_type == frozendict
        )  # accept empty dicts

    @property
    def py_type(self) -> Type:
        return self.py_collection_type[self.key_type.py_type, self.value_type.py_type]

    @property
    def is_resolved(self) -> bool:
        return self.key_type.is_resolved and self.value_type.is_resolved

    @property
    def type_vars(self):
        return self.key_type.type_vars | self.value_type.type_vars

    @property
    def generic_rank(self) -> dict[type, float]:
        return combine_ranks((self.key_type.generic_rank, self.value_type.generic_rank), 0.01)

    @classmethod
    def parse_type(cls, value_tp) -> "HgScalarTypeMetaData":
        if isinstance(value_tp, (GenericAlias, _GenericAlias)) and value_tp.__origin__ in [frozendict, dict, Mapping]:
            if (key_tp := HgScalarTypeMetaData.parse_type(value_tp.__args__[0])) and (
                value_tp := HgScalarTypeMetaData.parse_type(value_tp.__args__[1])
            ):
                return HgDictScalarType(key_tp, value_tp)

    @class_or_instance_method
    def parse_value(self, value) -> "HgScalarTypeMetaData":
        if isinstance(value, (dict, frozendict)):
            if len(value) > 0:
                key, value = next(iter(value.items()))
                key_tp = HgScalarTypeMetaData.parse_value(key)
                value_tp = HgScalarTypeMetaData.parse_value(value)
                if key_tp and value_tp:
                    return HgDictScalarType(key_tp, value_tp)
            elif isinstance(self, HgDictScalarType):
                return self

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        else:
            return HgDictScalarType(
                self.key_type.resolve(resolution_dict, weak), self.value_type.resolve(resolution_dict, weak)
            )

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        super().do_build_resolution_dict(resolution_dict, wired_type)
        wired_type: HgDictScalarType
        self.key_type.build_resolution_dict(resolution_dict, wired_type.key_type)
        self.value_type.build_resolution_dict(resolution_dict, wired_type.value_type)

    def __eq__(self, o: object) -> bool:
        return type(o) is HgDictScalarType and self.key_type == o.key_type and self.value_type == o.value_type

    def __str__(self) -> str:
        return f"Mapping[{str(self.key_type)}, {str(self.value_type)}]"

    def __repr__(self) -> str:
        return f"HgDictScalarType({repr(self.key_type)}, {repr(self.value_type)})"

    def __hash__(self) -> int:
        return hash(dict) ^ hash(self.key_type) ^ hash(self.value_type)


class HgCompoundScalarType(HgScalarTypeMetaData):
    is_atomic = False  # This has the __meta_data_schema__ associated to the type with additional type information.

    @property
    def meta_data_schema(self) -> dict[str, "HgScalarTypeMetaData"]:
        if _is_dataclass_scalar_type(self.py_type):
            return _dataclass_scalar_schema(self.py_type)
        return self.py_type.__meta_data_schema__

    def __init__(self, py_type: Type):
        self.py_type = py_type

    @cpp_type_property
    def cpp_type(self, _hgraph):
        # Detect recursion - for recursive compound scalars, fall back to Python handling
        if SchemaRecurseContext.is_in_context(self.py_type):
            return None
        # Plain dataclasses retain their Python identity and always use opaque storage.
        if not _is_dataclass_scalar_type(self.py_type) and getattr(self.py_type, "__cpp_native__", False):
            return self._get_expanded_cpp_type()
        else:
            return self._get_opaque_cpp_type()

    def _get_expanded_cpp_type(self):
        """Returns field-expanded Bundle TypeMeta with CompoundScalarOps.

        This stores each field in C++ memory and reconstructs the Python class
        in to_python(). Use this when you need efficient field access from C++.
        """
        try:
            import hgraph._hgraph as _hgraph

            # Build the fields list with (name, type_meta) pairs
            with SchemaRecurseContext(self.py_type):
                fields = []
                schema = self.meta_data_schema
                for field_name, field_type_meta in schema.items():
                    field_cpp = field_type_meta.cpp_type
                    if field_cpp is None:
                        return None  # Fall back to nb::object if any field type is not supported
                    fields.append((field_name, field_cpp))
                # Create CompoundScalar TypeMeta with Python class for reconstruction
                return _hgraph.value.get_compound_scalar_type_meta(fields, self.py_type, self.py_type.__name__)
        except (ImportError, AttributeError):
            return None

    def _get_opaque_cpp_type(self):
        """Returns nb::object TypeMeta for opaque Python object storage.

        This preserves Python object identity, which is important for hashing
        and equality. Use this (the default) when you need to preserve object
        identity or when the CompoundScalar has complex subclass behavior.
        """
        try:
            import hgraph._hgraph as _hgraph

            return _hgraph.value.get_scalar_type_meta(object)
        except (ImportError, AttributeError):
            return None

    def __eq__(self, o: object) -> bool:
        if SchemaRecurseContext.is_in_context(self.py_type):
            return True

        with SchemaRecurseContext(self.py_type):
            if type(o) is not HgCompoundScalarType:
                return False
            if self.py_type is o.py_type or self.py_type == o.py_type:
                return True
            if _is_dataclass_scalar_type(self.py_type) or _is_dataclass_scalar_type(o.py_type):
                return False
            return (
                issubclass(self.py_type, UnNamedCompoundScalar) or issubclass(o.py_type, UnNamedCompoundScalar)
            ) and self.meta_data_schema == o.meta_data_schema

    def __str__(self) -> str:
        return getattr(self.py_type, "__name__", str(self.py_type))

    def __repr__(self) -> str:
        return f"HgCompoundScalarType({repr(self.py_type)})"

    def __hash__(self) -> int:
        return hash(self.py_type)

    @property
    def type_vars(self):
        if SchemaRecurseContext.is_in_context(self.py_type):
            return set()

        with SchemaRecurseContext(self.py_type):
            if _is_dataclass_scalar_type(self.py_type):
                # Only TypeVars declared by Generic[...] can form a nominal
                # dataclass specialization. Free TypeVars in annotations are
                # descriptive (STATE schemas use this pattern).
                return set(getattr(self.py_type, "__parameters__", ()))
            return set().union(*(t.type_vars for t in self.meta_data_schema.values())) | set(
                getattr(self.py_type, "__parameters__", ())
            )

    @property
    def generic_rank(self) -> dict[type, float]:
        if SchemaRecurseContext.is_in_context(self.py_type):
            return {}

        with SchemaRecurseContext(self.py_type):
            if _is_dataclass_scalar_type(self.py_type):
                origin = _dataclass_scalar_origin(self.py_type)
                hierarchy = [
                    cls
                    for cls in origin.__mro__
                    if cls is not object and is_dataclass(cls) and not issubclass(cls, AbstractSchema)
                ]
                inheritance_depth = len(hierarchy)
                hierarchy_root = hierarchy[-1]
            else:
                inheritance_depth = self.py_type.__mro__.index(CompoundScalar)
                hierarchy_root = self.py_type.__mro__[inheritance_depth - 1]
            hierarchy_rank = {hierarchy_root: 1e-10 / inheritance_depth}

            generic_rank = combine_ranks((HgScalarTypeVar.parse_type(tp).generic_rank for tp in self.type_vars), 0.01)

            return generic_rank | hierarchy_rank

    def matches(self, tp: "HgTypeMetaData") -> bool:
        if type(tp) is not HgCompoundScalarType:
            return False
        if self.__eq__(tp):
            return True
        if _is_dataclass_scalar_type(self.py_type) and _is_dataclass_scalar_type(tp.py_type):
            self_origin, self_args = _structured_scalar_origin_and_args(self.py_type)
            tp_origin, _ = _structured_scalar_origin_and_args(tp.py_type)
            if self_args:
                if any(isinstance(argument, TypeVar) for argument in self_args):
                    return issubclass(tp_origin, self_origin) and all(
                        field_type.matches(tp.meta_data_schema[name])
                        for name, field_type in self.meta_data_schema.items()
                    )
                return _inherits_dataclass_specialization(tp.py_type, self_origin, self_args)
            return issubclass(tp_origin, self_origin)
        if _is_dataclass_scalar_type(self.py_type) or _is_dataclass_scalar_type(tp.py_type):
            return False
        return issubclass(tp.py_type, self.py_type)

    @property
    def is_resolved(self) -> bool:
        if (r := getattr(self, "_is_resolved", None)) is not None:
            return r

        if SchemaRecurseContext.is_in_context(self.py_type):
            return True

        with SchemaRecurseContext(self.py_type):
            resolved = (
                not getattr(self.py_type, "__parameters__", ())
                if _is_dataclass_scalar_type(self.py_type)
                else all(tp.is_resolved for tp in self.meta_data_schema.values())
                and not getattr(self.py_type, "__parameters__", False)
            )
            object.__setattr__(self, "_is_resolved", resolved)
            return resolved

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._scalar_types import CompoundScalar

        if (type(value_tp) is type and issubclass(value_tp, CompoundScalar)) or _is_dataclass_scalar_type(value_tp):
            return HgCompoundScalarType(value_tp)

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        from hgraph._types._scalar_types import CompoundScalar

        if isinstance(value, CompoundScalar) or _dataclass_scalar_is_instance(value):
            return HgCompoundScalarType(type(value))

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        elif _is_dataclass_scalar_type(self.py_type):
            origin, current_args = _structured_scalar_origin_and_args(self.py_type)
            parameters = tuple(getattr(origin, "__parameters__", ()))
            if not parameters:
                return self
            arguments = current_args or parameters
            resolved_arguments = []
            for argument in arguments:
                argument_meta = HgScalarTypeMetaData.parse_type(argument)
                resolved = argument_meta.resolve(resolution_dict, weak)
                resolved_arguments.append(resolved.py_type)
            if weak and any(isinstance(argument, TypeVar) for argument in resolved_arguments):
                return self
            item = resolved_arguments[0] if len(resolved_arguments) == 1 else tuple(resolved_arguments)
            return HgCompoundScalarType(origin[item])
        else:
            return HgCompoundScalarType(self.py_type._resolve(resolution_dict))

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if SchemaRecurseContext.is_in_context(self.py_type):
            return

        with SchemaRecurseContext(self.py_type):
            super().do_build_resolution_dict(resolution_dict, wired_type)
            wired_type: HgCompoundScalarType
            if any(k not in wired_type.meta_data_schema for k in self.meta_data_schema.keys()):
                raise ParseError("Keys of schema do not match")
            for v, w_v in ((v, wired_type.meta_data_schema[k]) for k, v in self.meta_data_schema.items()):
                v.build_resolution_dict(resolution_dict, w_v)


class HgCompoundScalarTypeForwardRef(HgScalarTypeMetaData):
    is_atomic = False  # This is a compound type

    @property
    def py_type(self) -> Type:
        if not hasattr(self, "_py_type"):
            raise AttributeError(f"{self._py_type_name} is not defined yet")
        return self._py_type

    @py_type.setter
    def py_type(self, value: Type):
        self._py_type = value
        self._hg_type = HgCompoundScalarType(value)

    @property
    def meta_data_schema(self) -> dict[str, "HgScalarTypeMetaData"]:
        return self.py_type.__meta_data_schema__

    def __init__(self, py_type_name: str):
        self._py_type_name = py_type_name

    def __eq__(self, o: object) -> bool:
        if hasattr(self, "_hg_type"):
            return (self._hg_type == o._hg_type) if type(o) is HgCompoundScalarTypeForwardRef else (self._hg_type == o)
        else:
            return type(o) is HgCompoundScalarTypeForwardRef and self._py_type_name == o._py_type_name

    def __str__(self) -> str:
        return f"{self.py_type.__name__}"

    def __repr__(self) -> str:
        return f"HgCompoundScalarType({repr(self.py_type)})"

    def __hash__(self) -> int:
        return hash(self.py_type)

    @property
    def type_vars(self):
        return self._hg_type.type_vars

    @property
    def generic_rank(self) -> dict[type, float]:
        return self._hg_type.generic_rank

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return self._hg_type.matches(tp._hg_type if type(tp) is HgCompoundScalarTypeForwardRef else tp)

    @property
    def is_resolved(self) -> bool:
        return self._hg_type.is_resolved if hasattr(self, "_hg_type") else True

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._scalar_types import CompoundScalar

        if type(value_tp) is type and issubclass(value_tp, CompoundScalar):
            return HgCompoundScalarType(value_tp)

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        from hgraph._types._scalar_types import CompoundScalar

        if isinstance(value, CompoundScalar):
            return HgCompoundScalarType(type(value))

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        return self._hg_type.resolve(resolution_dict, weak)

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        self._hg_type.do_build_resolution_dict(resolution_dict, wired_type)


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

    @property
    def type_vars(self):
        return self.value_tp.type_vars

    @property
    def generic_rank(self) -> dict[type, float]:
        return self.value_tp.generic_rank

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
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        if isinstance(value_tp, (_GenericAlias, GenericAlias)) and value_tp.__origin__ in (type, Type):
            value_tp = HgTypeMetaData.parse_type(value_tp.__args__[0])
            return HgTypeOfTypeMetaData(value_tp)

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        if isinstance(value, type):
            return HgTypeOfTypeMetaData(HgTypeMetaData.parse_type(value))

    def __eq__(self, o: object) -> bool:
        return type(o) is HgTypeOfTypeMetaData and self.value_tp == o.value_tp

    def __str__(self) -> str:
        return str(self.py_type)

    def __repr__(self) -> str:
        return f"HgTypeOfTypeMetaData({repr(self.value_tp)})"

    def __hash__(self) -> int:
        return hash(self.py_type)
