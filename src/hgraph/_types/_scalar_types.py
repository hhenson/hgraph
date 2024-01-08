from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, TypeVar
from typing import TypeVar, Type

from frozendict import frozendict

from hgraph._types._typing_utils import clone_typevar
from hgraph._types._schema_type import AbstractSchema

if TYPE_CHECKING:
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
    from hgraph._types._type_meta_data import HgTypeMetaData


__all__ = ("SCALAR", "UnSet", "Size", "SIZE",  "COMPOUND_SCALAR", "SCALAR", "CompoundScalar", "is_scalar",
           "is_compound_scalar", "STATE", "SCALAR_1", "SCALAR_2", "NUMBER")


class _UnSet:
    """
    The marker class to indicate that value is not present.
    """


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
            return HgScalarTypeMetaData.parse(tp)


UnSet = _UnSet()  # The marker instance to indicate the value is not set.
SIZE = TypeVar("SIZE", bound=Size)
COMPOUND_SCALAR = TypeVar("COMPOUND_SCALAR", bound=CompoundScalar)
SCALAR = TypeVar("SCALAR", bool, int, float, date, datetime, time, timedelta, str, tuple, frozenset, frozendict, _UnSet,
                 CompoundScalar)
SCALAR_1 = clone_typevar(SCALAR, "SCALAR_1")
SCALAR_2 = clone_typevar(SCALAR, "SCALAR_2")
NUMBER = TypeVar("NUMBER", int, float)


class STATE(dict):
    """
    State is basically just a dictionary.
    Add the ability to access the state as attributes.
    """

    def __getattr__(self, item):
        return self[item]

    def __setattr__(self, key, value):
        self[key] = value


def is_scalar(value) -> bool:
    """
    Is this value a supported scalar type. Not all python types are valid scalar types.
    This is a first pass estimate, and does not do a deep parse on container classes.
    This is not a substitute for HgScalarType.parse.
    """
    return isinstance(value, (bool, int, float, date, datetime, time, timedelta, str, tuple, frozenset, frozendict,
                              CompoundScalar, Size)) or (
        isinstance(value, type) and ( value in (bool, int, float, date, datetime, time, timedelta, str) or
                                      issubclass(value, (tuple, frozenset, frozendict, CompoundScalar, Size)) )
    )


def is_compound_scalar(value) -> bool:
    """Is the value an instance of CompoundScalar or is a type which is a subclass of CompoundScalar"""
    return isinstance(value, CompoundScalar) or (isinstance(value, type) and issubclass(value, CompoundScalar))

