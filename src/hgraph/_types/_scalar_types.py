from dataclasses import asdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from logging import Logger
from types import GenericAlias
from typing import TYPE_CHECKING, runtime_checkable, Protocol, Generic, Any, KeysView, ItemsView, ValuesView, Union, \
    _GenericAlias, Mapping
from typing import TypeVar, Type

from frozendict import frozendict

from hgraph._runtime._evaluation_engine import EvaluationEngineApi, EvaluationMode
from hgraph._types._schema_type import AbstractSchema
from hgraph._types._typing_utils import clone_typevar, nth

if TYPE_CHECKING:
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
    from hgraph._types._type_meta_data import HgTypeMetaData, ParseError

__all__ = ("SCALAR", "UnSet", "Size", "SIZE", "COMPOUND_SCALAR", "SCALAR", "CompoundScalar", "is_keyable_scalar",
           "is_compound_scalar", "STATE", "SCALAR_1", "SCALAR_2", "NUMBER", "KEYABLE_SCALAR", "LOGGER", "REPLAY_STATE",
           "compound_scalar", "UnNamedCompoundScalar")


class _UnSet:
    """
    The marker class to indicate that value is not present.
    """

    def __str__(self):
        return "<UnSet>"

    def __repr__(self):
        return "<UnSet>"


__CACHED_SIZES__: dict[int, Type["Size"]] = {}


class Size:
    """
    Size class, used by TSL to indicate the size attributes of the size.
    This represents either a fixed size or a variable size.

    Use this as Size[n] where n is the size represented as an integer value.
    """
    SIZE: int = -1  # NOSONAR
    FIXED_SIZE: bool = False

    @classmethod
    def __class_getitem__(cls, item):
        assert type(item) is int
        global __CACHED_SIZES__
        tp = __CACHED_SIZES__.get(item)
        if tp is None:
            tp = type(f"Size_{item}", (Size,), {'SIZE': item, 'FIXED_SIZE': True})
            __CACHED_SIZES__[item] = tp
        return tp

    def __str__(self):
        return f"Size[{str(self.SIZE) if self.FIXED_SIZE else ''}]"  # NOSONAR


class CompoundScalar(AbstractSchema):

    @classmethod
    def _parse_type(cls, tp: Type) -> "HgTypeMetaData":
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        return HgScalarTypeMetaData.parse_type(tp)

    def to_dict(self):
        d = {}
        for k in self.__meta_data_schema__:
            v = getattr(self, k, None)
            if isinstance(v, CompoundScalar):
                v = v.to_dict()
            if v is not None:
                d[k] = v
        return d


class UnNamedCompoundScalar(CompoundScalar):
    """Use this class to create un-named compound schemas"""

    def __init__(self, **kwargs):
        super().__init__()
        for k, v in kwargs.items():
            if k in self.__meta_data_schema__:
                object.__setattr__(self, k, v)
            else:
                raise ValueError(f"{k} is not defined in schema: {self.__meta_data_schema__}")

    @classmethod
    def create(cls, **kwargs) -> Type["UnNamedCompoundScalar"]:
        """Creates a type instance with root class UnNamedCompoundScalar using the kwargs provided"""
        from hgraph._types._time_series_meta_data import HgScalarTypeMetaData
        schema = {k: HgScalarTypeMetaData.parse_type(v) for k, v in kwargs.items()}
        if any(v is None for v in schema.values()):
            bad_inputs = {k: v for k, v in kwargs.items() if schema[k] is None}
            from hgraph._wiring._wiring_errors import CustomMessageWiringError
            raise CustomMessageWiringError(f"The following inputs are not valid scalar types: {bad_inputs}")
        return cls.create_resolved_schema(schema)

    @classmethod
    def create_resolved_schema(cls, schema: Mapping[str, "HgScalarTypeMetaData"]) \
            -> Type["UnNamedCompoundScalar"]:
        """Creates a type instance with root class UnNamedCompoundSchema using the schema provided"""
        return cls._create_resolved_class(schema)


def compound_scalar(**kwargs) -> Type["CompoundScalar"]:
    """
    Creates an un-named time-series schema using the kwargs provided.
    """
    return UnNamedCompoundScalar.create(**kwargs)


@runtime_checkable
class Hashable(Protocol):

    def __eq__(self, other):
        ...

    def __hash__(self):
        ...


UnSet = _UnSet()  # The marker instance to indicate the value is not set.
SIZE = TypeVar("SIZE", bound=Size)
COMPOUND_SCALAR = TypeVar("COMPOUND_SCALAR", bound=CompoundScalar)
COMPOUND_SCALAR_1 = clone_typevar(COMPOUND_SCALAR, "COMPOUND_SCALAR_1")
COMPOUND_SCALAR_2 = clone_typevar(COMPOUND_SCALAR, "COMPOUND_SCALAR_2")
SCALAR = TypeVar("SCALAR", bound=object)
KEYABLE_SCALAR = TypeVar("KEYABLE_SCALAR", bound=Hashable)
SCALAR_1 = clone_typevar(SCALAR, "SCALAR_1")
SCALAR_2 = clone_typevar(SCALAR, "SCALAR_2")
NUMBER = TypeVar("NUMBER", int, float, Decimal)


class STATE(Generic[COMPOUND_SCALAR]):
    """
    State is basically just a dictionary.
    Add the ability to access the state as attributes.
    """

    def __init__(self, __schema__: type[COMPOUND_SCALAR] = None, **kwargs):
        self.__schema__: type[COMPOUND_SCALAR] = __schema__
        self._updated: bool = False # Dirty flag, useful for tracking updates when persisting.
        self._value: COMPOUND_SCALAR = dict(**kwargs) if __schema__ is None else __schema__(**kwargs)

    def __class_getitem__(cls, item) -> Any:
        # For now limit to validation of item
        out = super(STATE, cls).__class_getitem__(item)
        if item is not COMPOUND_SCALAR:
            from hgraph._types._type_meta_data import HgTypeMetaData
            if not (tp := HgTypeMetaData.parse_type(item)).is_scalar:
                raise ParseError(
                    f"Type '{item}' must be a CompoundScalar or a valid TypeVar (bound to to CompoundScalar)")
            # if tp.is_resolved:
            #
            #     out = functools.partial(out, __schema__=item)
        return out

    @property
    def as_schema(self) -> COMPOUND_SCALAR:
        """
        Exposes the TSB as the schema type. This is useful for type completion in tools such as PyCharm / VSCode.
        It is a convenience method, it is possible to access the properties of the schema directly from the TSB
        instances as well.
        """
        return self.__dict__["_value"]

    def __getattr__(self, item):
        """
        The time-series value for the property associated to item in the schema
        :param item:
        :return:
        """
        values = self.__dict__.get("_value")
        schema: COMPOUND_SCALAR = self.__dict__.get("__schema__")
        if item == "_value":
            if values is None:
                raise AttributeError(item)
            return values
        if item == "__schema__":
            if schema is None:
                raise AttributeError(item)
            return schema
        if schema is None:
            if values and item in values:
                return values[item]
            else:
                raise AttributeError(item)
        else:
            if not hasattr(schema, "__meta_data_schema__") or item in schema.__meta_data_schema__:
                return getattr(values, item)
            else:
                raise AttributeError(item)

    def __getitem__(self, item: Union[int, str]) -> SCALAR:
        """
        If item is of type int, will return the item defined by the sequence of the schema. If it is a str, then
        the item as named.
        """
        if type(item) is int:
            return self.__dict__["_value"][nth(iter(self.__schema__.__meta_data_schema__), item)]
        else:
            return getattr(self, item)

    def keys(self) -> KeysView[str]:
        """The keys of the schema defining the bundle"""
        return self.__dict__["_value"].keys()

    def items(self) -> ItemsView[str, SCALAR]:
        """The items of the bundle"""
        return self.__dict__["_value"].items()

    def values(self) -> ValuesView[SCALAR]:
        """The values of the bundle"""
        return self.__dict__["_value"].values()

    def __setattr__(self, key, value):
        if key in ["_value", "__schema__", "_updated"]:
            self.__dict__[key] = value
        else:
            value_ = self.__dict__["_value"]
            schema = self.__dict__["__schema__"]
            self.__dict__["_updated"] = True
            if schema is None:
                value_[key] = value
            else:
                setattr(value_, key, value)

    def reset_updated(self) -> None:
        """Resets the updated state back to false"""
        self.__dict__["_updated"] = False

    def is_updated(self) -> bool:
        """Has the state been updated since last reset / created"""
        return self.__dict__["_updated"]

    def __repr__(self) -> str:
        return f"SCALAR[{self.__schema__.__name__}]({', '.join(k + '=' + repr(v) for k, v in asdict(self._value).items())})"


class REPLAY_STATE:
    """
    Used to indicate if the graph is currently been replayed. This serves two purposes, one to know if the graph
    is currently replaying and another to indicate that the node is replay aware. If this is a sink-node it will
    be evaluated during replay, if not it will be excluded.
    """

    def __init__(self, api: EvaluationEngineApi):
        self._api: EvaluationEngineApi = api

    @property
    def is_replaying(self) -> bool:
        return self._api.evaluation_mode == EvaluationMode.REPLAY

    def __repr__(self) -> str:
        return "REPLAY_STATE"


LOGGER = Logger


def is_keyable_scalar(value) -> bool:
    """
    Is this value a supported scalar type. Not all python types are valid scalar types.
    This is a first pass estimate, and does not do a deep parse on container classes.
    This is not a substitute for HgScalarType.parse.
    """
    return (
            isinstance(value, (bool, int, float, date, datetime, time, timedelta, str, tuple, frozenset, frozendict,
                              CompoundScalar, Size, Enum))
            or
            (isinstance(value, type) and (
                    value in (bool, int, float, date, datetime, time, timedelta, str)
                    or
                    issubclass(value, (tuple, frozenset, frozendict, CompoundScalar, Size, Enum))))
            or
            (isinstance(value, TypeVar) and (
                        is_keyable_scalar(value.__bound__) or all(is_keyable_scalar(v) for v in value.__constraints__)
                        ))
            or
            (isinstance(value, (GenericAlias, _GenericAlias)) and (
                        is_keyable_scalar(value.__origin__) and all(is_keyable_scalar(v) for v in value.__args__)
                        ))
    )


def is_compound_scalar(value) -> bool:
    """Is the value an instance of CompoundScalar or is a type which is a subclass of CompoundScalar"""
    return isinstance(value, CompoundScalar) or (isinstance(value, type) and issubclass(value, CompoundScalar))
