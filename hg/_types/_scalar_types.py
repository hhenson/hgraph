from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import TypeVar, Type

__all__ = ("SCALAR", "UnSet", "Size", "SIZE",  "COMPOUND_SCALAR", "CompoundScalar")

from typing import TYPE_CHECKING

from frozendict import frozendict

from hg._types._schema_type import AbstractSchema

if TYPE_CHECKING:
    from hg._types._scalar_type_meta_data import HgScalarTypeMetaData


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
    SIZE: int = -1
    FIXED_SIZE: bool = False

    @classmethod
    def __class_getitem__(cls, item):
        assert type(item) is int
        global __CACHED_SIZES__
        tp = __CACHED_SIZES__.get(item)
        if tp is None:
            __CACHED_SIZES__[item] = (tp := type(f"Size_{item}", (Size,), {'SIZE': item, 'FIXED_SIZE': True}))
        return tp


class CompoundScalar(AbstractSchema):

        @classmethod
        def _parse_type(cls, tp: Type) -> "HgTypeMetaData":
            from hg._types._scalar_type_meta_data import HgScalarTypeMetaData
            return HgScalarTypeMetaData.parse(tp)


UnSet = _UnSet()  # The marker instance to indicate the value is not set.
SIZE = TypeVar("SIZE", bound=Size)
COMPOUND_SCALAR = TypeVar("COMPOUND_SCALAR", bound=CompoundScalar)
SCALAR = TypeVar("SCALAR", bool, int, float, date, datetime, time, timedelta, str, tuple, frozenset, frozendict, _UnSet,
                 CompoundScalar)


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