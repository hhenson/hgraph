from enum import Enum, auto
from typing import Type

from hgraph._types import OUT, TIME_SERIES_TYPE, TS, SCALAR, TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2
from hgraph._types._scalar_types import Size, SIZE, DEFAULT
from hgraph._types._tsl_type import TSL
from hgraph._wiring._decorators import operator, graph
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass
from hgraph._wiring._wiring_port import WiringPort

__all__ = (
    "CmpResult",
    "DivideByZero",
    "abs_",
    "accumulate",
    "add_",
    "and_",
    "average",
    "bit_and",
    "bit_or",
    "bit_xor",
    "contains_",
    "cmp_",
    "difference",
    "div_",
    "divmod_",
    "eq_",
    "floordiv_",
    "ge_",
    "getattr_",
    "getitem_",
    "gt_",
    "index_of",
    "intersection",
    "invert_",
    "is_empty",
    "le_",
    "len_",
    "ln",
    "lshift_",
    "lt_",
    "max_",
    "mean",
    "min_",
    "mod_",
    "mul_",
    "ne_",
    "neg_",
    "not_",
    "or_",
    "pos_",
    "pow_",
    "rshift_",
    "setattr_",
    "std",
    "str_",
    "sub_",
    "sum_",
    "symmetric_difference",
    "type_",
    "union",
    "var",
    "zero",
)


@operator
def add_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> DEFAULT[OUT]:
    """
    This represents the ``+`` operator for time series types.
    """


WiringPort.__add__ = lambda x, y: add_(x, y)
WiringPort.__radd__ = lambda x, y: add_(y, x)


@operator
def sub_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> DEFAULT[OUT]:
    """
    This represents the ``-`` operator for time series types.
    """


WiringPort.__sub__ = lambda x, y: sub_(x, y)
WiringPort.__rsub__ = lambda x, y: sub_(y, x)


@operator
def mul_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE, **kwargs) -> TIME_SERIES_TYPE:
    """
    This represents the ``*`` operator for time series types.
    Parameters:
    __strict__: bool: (default False)
    * if True will return nothing if either lhs or rhs is not valid or present
    * if False will return lhs or rhs if rhs or lhs respectively is not present
    """


WiringPort.__mul__ = lambda x, y: mul_(x, y)
WiringPort.__rmul__ = lambda x, y: mul_(y, x)


class DivideByZero(Enum):
    """For numeric division set the divide_by_zero property"""

    ERROR = auto()
    NAN = auto()
    INF = auto()
    NONE = auto()
    ZERO = auto()
    ONE = auto()


@operator
def div_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE, **kwargs) -> DEFAULT[OUT]:
    """
    This represents the `/` operator for time series types.

    Parameters:
    * divide_by_zero: DivideByZero - controls the behaviour when dividing by zero
    """


WiringPort.__truediv__ = lambda x, y: div_(x, y)
WiringPort.__rtruediv__ = lambda x, y: div_(y, x)


@operator
def floordiv_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE, **kwargs) -> TIME_SERIES_TYPE:
    """
    This represents the `//` operator for time series types.
    """


WiringPort.__floordiv__ = lambda x, y: floordiv_(x, y)
WiringPort.__rfloordiv__ = lambda x, y: floordiv_(y, x)


@operator
def mod_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `%` operator for time series types.
    """


WiringPort.__mod__ = lambda x, y: mod_(x, y)
WiringPort.__rmod__ = lambda x, y: mod_(y, x)


@operator
def divmod_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TSL[TIME_SERIES_TYPE, Size[2]]:
    """
    This represents the `divmod` operator for time series types.
    (This is defined in Python as the integer division with remainder, i.e. divmod(9, 4) == (2, 1))
    """


WiringPort.__divmod__ = lambda x, y: divmod_(x, y)
WiringPort.__rdivmod__ = lambda x, y: divmod_(y, x)


@operator
def pow_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `**` operator for time series types.
    Params:
    * divide_by_zero: DivideByZero - controls the behaviour when dividing by zero
    """


WiringPort.__pow__ = lambda x, y: pow_(x, y)
WiringPort.__rpow__ = lambda x, y: pow_(y, x)


@operator
def lshift_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the ``<<`` operator for time series types.
    """


WiringPort.__lshift__ = lambda x, y: lshift_(x, y)
WiringPort.__rlshift__ = lambda x, y: lshift_(y, x)


@operator
def rshift_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the ``>>`` operator for time series types.
    """


WiringPort.__rshift__ = lambda x, y: rshift_(x, y)
WiringPort.__rrshift__ = lambda x, y: rshift_(y, x)


@operator
def bit_and(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> DEFAULT[OUT]:
    """
    This represents the ``&`` operator for time series types.
    """


WiringPort.__and__ = lambda x, y: bit_and(x, y)
WiringPort.__rand__ = lambda x, y: bit_and(y, x)


@operator
def and_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the ``and`` operator for time series types.

    This operator does not substitute ``and`` (since that is not possible in Python), but can be used as a functional
    equivalent for ``and``.
    """


@operator
def bit_or(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[TIME_SERIES_TYPE_1]:
    """
    This represents the ``|`` operator for time series types.
    """


WiringPort.__or__ = lambda x, y: bit_or(x, y)
WiringPort.__ror__ = lambda x, y: bit_or(y, x)


@operator
def or_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the ``or`` operator for time series types.

    This operator does not substitute ``or`` (since that is not possible in Python), but can be used as a functional
    equivalent for ``or``.
    """


@operator
def bit_xor(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> DEFAULT[OUT]:
    """
    This represents the ``^`` operator for time series types.
    """


WiringPort.__xor__ = lambda x, y: bit_xor(x, y)
WiringPort.__rxor__ = lambda x, y: bit_xor(y, x)


@operator
def eq_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the ``==`` operator for time series types.
    """


# This is currently safe to do as the wiring port needs to be immutable, but is never used as a key in a dict or
# compared to another port. But in case we need access to the original store it back on the class.
WiringPort.__orig_eq__ = WiringPort.__eq__
WiringPort.__eq__ = lambda x, y: eq_(x, y)


@operator
def ne_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the ``!=`` operator for time series types.
    """


WiringPort.__ne__ = lambda x, y: ne_(x, y)


@operator
def lt_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the ``<`` operator for time series types.
    """


WiringPort.__lt__ = lambda x, y: lt_(x, y)


@operator
def le_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the ``<=`` operator for time series types.
    """


WiringPort.__le__ = lambda x, y: le_(x, y)


@operator
def gt_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the ``>`` operator for time series types.
    """


WiringPort.__gt__ = lambda x, y: gt_(x, y)


@operator
def ge_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the ``>=`` operator for time series types.
    """


WiringPort.__ge__ = lambda x, y: ge_(x, y)


@operator
def neg_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the unary ``-`` operator for time series types.
    """


WiringPort.__neg__ = lambda x: neg_(x)


@operator
def pos_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the unary ``+`` operator for time series types.
    """


WiringPort.__pos__ = lambda x: pos_(x)


@operator
def abs_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the ``abs`` operator for time series types.
    """


WiringPort.__abs__ = lambda x: abs_(x)


@operator
def invert_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the unary ``~`` operator for time series types.
    """


WiringPort.__invert__ = lambda x: invert_(x)


@operator
def contains_(ts: TIME_SERIES_TYPE, item: TS[SCALAR]) -> TS[bool]:
    """
    This represents the `in` operator for time series types, however, since ``__contains__`` always returns a bool
    value, we can't overload the ``__contains__``, so it is not possible to do ``item in ts``, instead use
    ``contains_(ts, item)``.

    This is logically: ``item in ts``
    """


@operator
def not_(ts: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the unary `not` operator for time series types.

    This must be called as ``not_(ts)`` it is not possible to overload the standard ``not`` operator.
    """


@operator
def getitem_(ts: TIME_SERIES_TYPE, key: TS[SCALAR]) -> TIME_SERIES_TYPE_1:
    """
    This represents the ``[]`` operator for time-series types.

    Use this as: ``ts[key]``
    """


WiringPort.__getitem__ = lambda x, y: getitem_(x, y)


@operator
def getattr_(ts: TIME_SERIES_TYPE, attr: str, default_value: SCALAR = None) -> TIME_SERIES_TYPE_1:
    """
    This represents the ``.`` operator for time-series types.

    Use this as: ``ts.attr`` or more explicitly: ``getattr_(ts, attr)``
    """


WiringPort.__getattr__ = lambda x, y: getattr_(x, y)


@operator
def setattr_(ts: OUT, attr: str, value: TIME_SERIES_TYPE_1) -> OUT:
    """
    Sets the value on the ``ts`` provided for the ``attr`` to the ``value`` provided.
    """


@operator
def min_(*ts: TSL[TS[SCALAR], SIZE], default_value: TS[SCALAR] = None, __strict__: bool = True) -> TIME_SERIES_TYPE:
    """
    This represents the ``min`` operator for time series types.

    * Unary implies the min over the latest TS value for collection types, or running min for non-collection types
        In the case of a running sum, a 'reset' signal may be provided, which resets the sum to zero when it ticks
    * Binary or multi arg implies item-wise min over all the arguments for collection types,
        or the minimum scalar value for scalar types

    __strict__ controls whether the operator will tick if any of the arguments are missing
    """


@operator
def max_(*ts: TSL[TS[SCALAR], SIZE], default_value: TS[SCALAR] = None, __strict__: bool = True) -> TIME_SERIES_TYPE:
    """
    The ``max`` operator for time series types.

    * Unary implies the max over the latest TS value for collection types, or running max for non-collection types
        In the case of a running sum, a 'reset' signal may be provided, which resets the sum to zero when it ticks
    * Binary or multi arg implies item-wise max over all the arguments for collection types,
        or the maximum scalar value for scalar types

    __strict__ controls whether the operator will tick if any of the arguments are missing
    """


@operator
def sum_(*ts: TSL[TS[SCALAR], SIZE], **kwargs) -> DEFAULT[OUT]:
    """
    This represents the ``sum`` operator for time series types, either as a binary or unary operator

    * Unary implies the sum over the latest TS value for collection types, or running sum for non-collection types.
        In the case of a running sum, a 'reset' signal may be provided, which resets the sum to zero when it ticks
    * Binary or multi arg implies item-wise sum over all the arguments for collection types,
        or the sum of the scalar value for scalar types
    """


@operator
def mean(*ts: TSL[TIME_SERIES_TYPE, SIZE]) -> DEFAULT[OUT]:
    """
    This represents the ``mean`` operator for time series types

    Unary implies the mean over the latest TS value for collection types, or running mean for non-collection types
    Binary or multi arg implies item-wise sum over all the arguments for collection types,
    or the sum of the scalar value for scalar types
    """


@operator
def std(*ts: TSL[TIME_SERIES_TYPE, SIZE]) -> DEFAULT[OUT]:
    """
    Calculates the standard deviation for time series types

    Unary implies the std over the latest TS value for collection types, or running std for non-collection types
    Binary or multi arg implies item-wise std over all the arguments for collection types,
    or the std of the scalar value for scalar types
    """


@operator
def var(*ts: TSL[TIME_SERIES_TYPE, SIZE]) -> DEFAULT[OUT]:
    """
    Calculates the variance for time series types

    Unary implies the var over the latest TS value for collection types, or running var for non-collection types
    Binary or multi arg implies item-wise var over all the arguments for collection types,
    or the vae of the scalar value for scalar types
    """


@operator
def zero(tp: Type[TIME_SERIES_TYPE], op: WiringNodeClass) -> TIME_SERIES_TYPE:
    """
    This is a helper graph to create a zero time-series (for example, for the reduce function). The zero values are
    type and operation dependent, so both are provided. The datatype designers should overload this graph for their
    respective data types and return correct zero values for the operation.
    """


@operator
def len_(ts: TIME_SERIES_TYPE) -> TS[int]:
    """
    This represents the `len` operator for time series types.
    """


@operator
def union(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> DEFAULT[OUT]:
    """
    Performs a union of the provided time-series values.

    Union is { p | p element of tsl[i] for i in range(len(tsl)) }
    """


@operator
def intersection(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> DEFAULT[OUT]:
    """
    Performs an intersection of the provided time-series values.

    Intersection is { p | p in all tsl[i] for i in range(len(tsl)) }
    """


@operator
def difference(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> DEFAULT[OUT]:
    """
    Performs a difference of the provided time-series values.

    Difference is { p | p element of lhs and p not element of rhs }
    """


@operator
def symmetric_difference(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> DEFAULT[OUT]:
    """
    Performs the symmetric difference of the provided time-series values.

    Symmetric difference is { p | p element of union(lhs, rhs), but not element of intersection(lhs, rhs) }
    """


@operator
def is_empty(ts: TIME_SERIES_TYPE) -> TS[bool]:
    """
    Returns True if the value of the time-series is considered empty, False otherwise.
    """


@operator
def type_(ts: TIME_SERIES_TYPE) -> TS[type]:
    """
    Returns the type of the time-series value.
    """


@operator
def str_(ts: TIME_SERIES_TYPE) -> TS[str]:
    """
    Returns the string representation of the time-series value.
    """


# For backwards compatibility.  Prefer sum_ to accumulate and mean to average
@graph(deprecated="Prefer sum_")
def accumulate(*ts: TSL[TS[SCALAR], SIZE], default_value: TS[SCALAR] = None) -> DEFAULT[OUT]:
    if default_value is not None:
        raise NotImplementedError(f"Accumulate is not implemented for {type(default_value)}")
    return sum_(*ts)


@graph(deprecated="Prefer mean")
def average(*ts: TSL[TIME_SERIES_TYPE, SIZE]) -> DEFAULT[OUT]:
    return mean(*ts)


class CmpResult(Enum):
    LT = -1
    EQ = 0
    GT = 1


@operator
def cmp_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[CmpResult]:
    """
    Return one of LT, EQ, GT as a comparison result.
    This could be more efficient than performing a sequence of operations.
    """


@operator
def index_of(ts: TIME_SERIES_TYPE_1, item: TIME_SERIES_TYPE_2) -> TS[int]:
    """
    Returns the index of a value within the ts provided.
    Options include:

    TS[tuple[SCALAR, ...]]
        returns the index of the first occurrence of the item in the tuple

    TSL[TIME_SERIES_TYPE_2, SIZE]
        returns the index of the first occurrence of the item in the TSL
    """


@operator
def ln(ts: TS[float]) -> TS[float]:
    """The natural logarithm of the time-series value"""
