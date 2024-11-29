from dataclasses import asdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from logging import Logger
from types import GenericAlias
from typing import (
    TYPE_CHECKING,
    runtime_checkable,
    Protocol,
    Generic,
    Any,
    KeysView,
    ItemsView,
    ValuesView,
    Union,
    _GenericAlias,
    Mapping,
)
from typing import TypeVar, Type

from frozendict import frozendict

from hgraph._types._schema_type import AbstractSchema
from hgraph._types._typing_utils import clone_type_var, nth

if TYPE_CHECKING:
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
    from hgraph._types._type_meta_data import HgTypeMetaData, ParseError

__all__ = (
    "WINDOW_SIZE",
    "WINDOW_SIZE_MIN",
    "WindowSize",
    "SCALAR",
    "Size",
    "SIZE",
    "SIZE_1",
    "COMPOUND_SCALAR",
    "SCALAR",
    "CompoundScalar",
    "is_keyable_scalar",
    "is_compound_scalar",
    "STATE",
    "SCALAR_1",
    "SCALAR_2",
    "NUMBER",
    "KEYABLE_SCALAR",
    "LOGGER",
    "compound_scalar",
    "UnNamedCompoundScalar",
    "COMPOUND_SCALAR_1",
    "COMPOUND_SCALAR_2",
    "DEFAULT",
    "NUMBER_2",
    "TUPLE",
    "ENUM",
)


class Default:
    """
    The marker class to indicate a type parameter that is the default if provided on a generic type without a key.
    Also doubles up as default choice in switch_
    """

    def __init__(self, tp: TypeVar):
        self.tp = tp

    def __class_getitem__(cls, item):
        return cls(item)


DEFAULT = Default


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
            tp = type(f"Size_{item}", (Size,), {"SIZE": item, "FIXED_SIZE": True})
            __CACHED_SIZES__[item] = tp
        return tp

    def __str__(self):
        return f"Size[{str(self.SIZE) if self.FIXED_SIZE else ''}]"  # NOSONAR


__CACHED_BUFF_SIZES__: dict[int, Type["WindowSize"]] = {}


class WindowSize:
    """
    WindowSize class is used to provide the buffer dimensions to the buffer class via the templating mechanism.
    WindowSize can represent a number of ticks to buffer or a time-delta to record.

    Use this as WindowSize[n] where n is the size represented as an integer value or a time-delta

    For example:
    ::

        @compute_node
        def my_node(...) -> TSW[int, WindowSize[63]]:
            ...

    or

        @compute_node
        def my_node(ts: TSW[int, WindowSize[timedelta(seconds=20)]]) -> TS[int]:
            ...

    """

    SIZE: int = -1  # NOSONAR
    FIXED_SIZE: bool = False
    TIME_RANGE: timedelta = None

    @classmethod
    def __class_getitem__(cls, item):
        global __CACHED_BUFF_SIZES__
        tp = __CACHED_BUFF_SIZES__.get(item)
        if tp is None:
            if type(item) is int:
                tp = type(f"WindowSize_{item}", (WindowSize,), {"SIZE": item, "FIXED_SIZE": True, "TIME_RANGE": None})
            elif type(item) is timedelta:
                tp = type(f"WindowSize_{item}", (WindowSize,), {"SIZE": -1, "FIXED_SIZE": False, "TIME_RANGE": item})
            else:
                raise TypeError(f"Unexpected type {type(item)}")
            __CACHED_BUFF_SIZES__[item] = tp
        return tp

    def __str__(self):
        return f"WindowSize[{str(self.SIZE) if self.FIXED_SIZE else self.TIME_RANGE}]"  # NOSONAR


class CompoundScalar(AbstractSchema):
    """
    Use this to construct scalar values with more complex structure. Below is an example of the use of this:

    ::

        @dataclass(frozen=True)
        class MyCompoundScalar(CompoundScalar):
            p1: str
            p2: int

    The compound scalar can contain other compound scalar types, but as of now it cannot support a recursive
    definition, i.e. not possible to have a property of type ``MyCompoundScalar``.

    It is possible to create a generic compound scalar, for example:

    ::

        @dataclass(frozen=True)
        class MyTemplateScalar(CompoundScalar, Generic[SCALAR]):
            p1: SCALAR


    """

    @classmethod
    def _parse_type(cls, tp: Type) -> "HgTypeMetaData":
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        return HgScalarTypeMetaData.parse_type(tp)

    def to_dict(self):
        """
        Converts the value of the compound scalar into a dictionary. This will allow for construction of the type
        using ``**kwargs`` pattern. For example:

        ::

            my_cs = MyCompundScalar(p1="test", p2=42)
            my_cs_copy = MyCompundScalar(**my_cs.to_dict())

        :return: The dictionary of the values.
        """
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
    def create_resolved_schema(cls, schema: Mapping[str, "HgScalarTypeMetaData"]) -> Type["UnNamedCompoundScalar"]:
        """Creates a type instance with root class UnNamedCompoundSchema using the schema provided"""
        return cls._create_resolved_class(schema)


def compound_scalar(**kwargs) -> Type["CompoundScalar"]:
    """
    Provides a mechanism to create a ``CompoundScalar`` instance without creating a class first. This is useful
    for either dynamically creating a compound scalar or when creating a convenience result. Here is an example
    of its use:

    ::

        @compute_node
        def combine_inputs(ts_1: TS[int], ts_2: TS[str]) -> TS[compound_scalar(p1=int, p2=str)]:
            return {"p1": ts_1.value, "p2": ts_2.value}

    """
    return UnNamedCompoundScalar.create(**kwargs)


@runtime_checkable
class Hashable(Protocol):

    def __eq__(self, other): ...

    def __hash__(self): ...


SIZE = TypeVar("SIZE", bound=Size)
SIZE_1 = clone_type_var(SIZE, "SIZE_1")

WINDOW_SIZE = TypeVar("WINDOW_SIZE", bound=WindowSize)
WINDOW_SIZE_MIN = TypeVar("WINDOW_SIZE_MIN", bound=WindowSize)

COMPOUND_SCALAR = TypeVar("COMPOUND_SCALAR", bound=CompoundScalar)
COMPOUND_SCALAR_1 = clone_type_var(COMPOUND_SCALAR, "COMPOUND_SCALAR_1")
COMPOUND_SCALAR_2 = clone_type_var(COMPOUND_SCALAR, "COMPOUND_SCALAR_2")
SCALAR = TypeVar("SCALAR", bound=object)
KEYABLE_SCALAR = TypeVar("KEYABLE_SCALAR", bound=Hashable)
SCALAR_1 = clone_type_var(SCALAR, "SCALAR_1")
SCALAR_2 = clone_type_var(SCALAR, "SCALAR_2")
NUMBER = TypeVar("NUMBER", int, float, Decimal)
NUMBER_2 = clone_type_var(NUMBER, "NUMBER_2")


class STATE(Generic[COMPOUND_SCALAR]):
    """
    State is basically just a dictionary.
    Add the ability to access the state as attributes.
    """

    def __init__(self, __schema__: type[COMPOUND_SCALAR] = None, **kwargs):
        self.__schema__: type[COMPOUND_SCALAR] = __schema__
        self._updated: bool = False  # Dirty flag, useful for tracking updates when persisting.
        self._value: COMPOUND_SCALAR = dict(**kwargs) if __schema__ is None else __schema__(**kwargs)

    def __class_getitem__(cls, item) -> Any:
        # For now limit to validation of item
        out = super(STATE, cls).__class_getitem__(item)
        if item is not COMPOUND_SCALAR:
            from hgraph._types._type_meta_data import HgTypeMetaData

            if not (tp := HgTypeMetaData.parse_type(item)).is_scalar:
                raise ParseError(f"Type '{item}' must be a CompoundScalar or a valid TypeVar (bound to CompoundScalar)")
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
        return (
            f"SCALAR[{self.__schema__.__name__}]({', '.join(k + '=' + repr(v) for k, v in asdict(self._value).items())})"
        )


LOGGER = Logger


def is_keyable_scalar(value) -> bool:
    """
    Is this value a supported scalar type. Not all python types are valid scalar types.
    This is a first pass estimate, and does not do a deep parse on container classes.
    This is not a substitute for HgScalarType.parse.
    """
    return (
        isinstance(
            value,
            (
                bool,
                int,
                float,
                date,
                datetime,
                time,
                timedelta,
                str,
                tuple,
                frozenset,
                frozendict,
                CompoundScalar,
                Size,
                Enum,
            ),
        )
        or (
            isinstance(value, type)
            and (
                value in (bool, int, float, date, datetime, time, timedelta, str)
                or issubclass(value, (tuple, frozenset, frozendict, CompoundScalar, Size, Enum))
            )
        )
        or (
            isinstance(value, TypeVar)
            and (is_keyable_scalar(value.__bound__) or all(is_keyable_scalar(v) for v in value.__constraints__))
        )
        or (
            isinstance(value, (GenericAlias, _GenericAlias))
            and (is_keyable_scalar(value.__origin__) and all(is_keyable_scalar(v) for v in value.__args__))
        )
    )


def is_compound_scalar(value) -> bool:
    """Is the value an instance of CompoundScalar or is a type, which is a subclass of CompoundScalar"""
    return isinstance(value, CompoundScalar) or (isinstance(value, type) and issubclass(value, CompoundScalar))


TUPLE = TypeVar("TUPLE", bound=tuple)

ZERO = object()

ENUM = TypeVar("ENUM", bound=Enum)
