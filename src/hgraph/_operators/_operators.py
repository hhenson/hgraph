from enum import Enum, auto
from typing import Type

from hgraph._types import TIME_SERIES_TYPE, TS, SCALAR, TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2
from hgraph._types._scalar_types import Size, SIZE
from hgraph._types._tsl_type import TSL
from hgraph._wiring._decorators import operator, graph
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass
from hgraph._wiring._wiring_port import WiringPort

__all__ = (
    "add_",
    "sub_",
    "mul_",
    "div_",
    "floordiv_",
    "mod_",
    "divmod_",
    "pow_",
    "lshift_",
    "rshift_",
    "and_",
    "or_",
    "bit_xor",
    "eq_",
    "ne_",
    "lt_",
    "le_",
    "gt_",
    "ge_",
    "neg_",
    "pos_",
    "abs_",
    "invert_",
    "contains_",
    "not_",
    "getitem_",
    "getattr_",
    "min_",
    "max_",
    "zero",
    "len_",
    "bit_and",
    "bit_or",
    "union",
    "intersection",
    "difference",
    "symmetric_difference",
    "is_empty",
    "type_",
    "sum_",
    "and_",
    "or_",
    "str_",
    "mean",
    "average",
    "accumulate",
    "std",
    "var",
    "DivideByZero"
)


@operator
def add_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> TIME_SERIES_TYPE_2:
    """
    This represents the `+` operator for time series types.
    To implement the add_ operator, do:
    ::

        @compute_node(overloads=add_)
        def my_add(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__add__ = lambda x, y: add_(x, y)
WiringPort.__radd__ = lambda x, y: add_(y, x)


@operator
def sub_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> TIME_SERIES_TYPE_2:
    """
    This represents the `-` operator for time series types.
    To implement the sub_ operator, do:
    ::

        @compute_node(overloads=sub_)
        def my_sub(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__sub__ = lambda x, y: sub_(x, y)
WiringPort.__rsub__ = lambda x, y: sub_(y, x)


@operator
def mul_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `*` operator for time series types.
    To implement the mul_ operator, do:
    ::

        @compute_node(overloads=mul_)
        def my_mul(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__mul__ = lambda x, y: mul_(x, y)
WiringPort.__rmul__ = lambda x, y: mul_(y, x)


class DivideByZero(Enum):
    """For numeric division set the divide_by_zero property"""
    ERROR = auto()
    NAN = auto()
    INF = auto()
    NONE = auto()


@operator
def div_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE, **kwargs) -> TIME_SERIES_TYPE_2:
    """
    This represents the `/` operator for time series types.
    To implement the div_ operator, do:
    ::

        @compute_node(overloads=div_)
        def my_div(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__truediv__ = lambda x, y: div_(x, y)
WiringPort.__rtruediv__ = lambda x, y: div_(y, x)


@operator
def floordiv_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE, **kwargs) -> TIME_SERIES_TYPE:
    """
    This represents the `//` operator for time series types.
    To implement the floordiv_ operator, do:
    ::

        @compute_node(overloads=floordiv_)
        def my_floordiv(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__floordiv__ = lambda x, y: floordiv_(x, y)
WiringPort.__rfloordiv__ = lambda x, y: floordiv_(y, x)


@operator
def mod_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `%` operator for time series types.
    To implement the mod_ operator, do:
    ::

        @compute_node(overloads=mod_)
        def my_mod(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__mod__ = lambda x, y: mod_(x, y)
WiringPort.__rmod__ = lambda x, y: mod_(y, x)


@operator
def divmod_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TSL[TIME_SERIES_TYPE, Size[2]]:
    """
    This represents the `divmod` operator for time series types.
    (This is defined in Python as the integer division with remainder, i.e. divmod(9, 4) == (2, 1))
    To implement the divmod_ operator, do:
    ::

        @compute_node(overloads=divmod_)
        def my_divmod(lhs: TS[MyType], rhs: TS[MyType]) -> TSL[TS[MyType], Size[2]]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__divmod__ = lambda x, y: divmod_(x, y)
WiringPort.__rdivmod__ = lambda x, y: divmod_(y, x)


@operator
def pow_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `**` operator for time series types.
    To implement the pow_ operator, do:
    ::

        @compute_node(overloads=pow_)
        def my_pow(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__pow__ = lambda x, y: pow_(x, y)
WiringPort.__rpow__ = lambda x, y: pow_(y, x)


@operator
def lshift_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `<<` operator for time series types.
    To implement the lshift_ operator, do:
    ::

        @compute_node(overloads=lshift_)
        def my_lshift(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__lshift__ = lambda x, y: lshift_(x, y)
WiringPort.__rlshift__ = lambda x, y: lshift_(y, x)


@operator
def rshift_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `>>` operator for time series types.
    To implement the rshift_ operator, do:
    ::

        @compute_node(overloads=rshift_)
        def my_rshift(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__rshift__ = lambda x, y: rshift_(x, y)
WiringPort.__rrshift__ = lambda x, y: rshift_(y, x)


@operator
def bit_and(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE_1:
    """
    This represents the `&` operator for time series types.

    To implement the & operator, do:
    ::

        @compute_node(overloads=bit_and)
        def my_and_op(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__and__ = lambda x, y: bit_and(x, y)
WiringPort.__rand__ = lambda x, y: bit_and(y, x)


@operator
def and_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `and` operator for time series types.

    This operator does not substitute ``and`` (since that is not possible in Python), but can be used as a functional
    equivalent for ``and``.

    To implement the and_ operator, do:
    ::

        @compute_node(overloads=and_)
        def my_and(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


@operator
def bit_or(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[TIME_SERIES_TYPE_1]:
    """
    This represents the `|` operator for time series types.

    To implement the or_ operator, do:
    ::

        @compute_node(overloads=bit_or)
        def my_or(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__or__ = lambda x, y: bit_or(x, y)
WiringPort.__ror__ = lambda x, y: bit_or(y, x)


@operator
def or_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `or` operator for time series types.

    This operator does not substitute ``or`` (since that is not possible in Python), but can be used as a functional
    equivalent for ``or``.

    To implement the or_ operator, do:
    ::

        @compute_node(overloads=or_)
        def my_or(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


@operator
def bit_xor(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE_1:
    """
    This represents the `^` operator for time series types.

    To implement the xor_ operator, do:
    ::

        @compute_node(overloads=bit_xor)
        def my_xor(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__xor__ = lambda x, y: bit_xor(x, y)
WiringPort.__rxor__ = lambda x, y: bit_xor(y, x)


@operator
def eq_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `==` operator for time series types.

    To implement the eq_ operator, do:
    ::

        @compute_node(overloads=eq_)
        def my_eq(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


# This is currently safe to do as the wiring port needs to be immutable, but is never used as a key in a dict or
# compared to another port. But in case we need access to the original store it back on the class.
WiringPort.__orig_eq__ = WiringPort.__eq__
WiringPort.__eq__ = lambda x, y: eq_(x, y)


@operator
def ne_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `!=` operator for time series types.

    To implement the ne_ operator, do:
    ::

        @compute_node(overloads=ne_)
        def my_ne(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__ne__ = lambda x, y: ne_(x, y)


@operator
def lt_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `<` operator for time series types.
    To implement the lt_ operator, do:
    ::

        @compute_node(overloads=lt_)
        def my_lt(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__lt__ = lambda x, y: lt_(x, y)


@operator
def le_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `<=` operator for time series types.

    To implement the le_ operator, do:
    ::

        @compute_node(overloads=le_)
        def my_le(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__le__ = lambda x, y: le_(x, y)


@operator
def gt_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `>` operator for time series types.

    To implement the gt_ operator, do:
    ::

        @compute_node(overloads=gt_)
        def my_gt(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__gt__ = lambda x, y: gt_(x, y)


@operator
def ge_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `>=` operator for time series types.

    To implement the ge_ operator, do:
    ::

        @compute_node(overloads=ge_)
        def my_ge(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__ge__ = lambda x, y: ge_(x, y)


@operator
def neg_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the unary `-` operator for time series types.

    To implement the neg_ operator, do:
    ::

        @compute_node(overloads=neg_)
        def my_neg(ts: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__neg__ = lambda x: neg_(x)


@operator
def pos_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the unary `+` operator for time series types.

    To implement the pos_ operator, do:
    ::

        @compute_node(overloads=pos_)
        def my_pos(ts: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__pos__ = lambda x: pos_(x)


@operator
def abs_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `abs` operator for time series types.

    To implement the abs_ operator, do:
    ::

        @compute_node(overloads=abs_)
        def my_abs(ts: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__abs__ = lambda x: abs_(x)


@operator
def invert_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the unary `~` operator for time series types.

    To implement the pos_ operator, do:
    ::

        @compute_node(overloads=pos_)
        def my_pos(ts: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__invert__ = lambda x: invert_(x)


@operator
def contains_(ts: TIME_SERIES_TYPE, item: TS[SCALAR]) -> TS[bool]:
    """
    This represents the `in` operator for time series types, however, since ``__contains__`` always returns a bool
    value, we can't overload the __contains__, so it is not possible to do ``item in ts``, instead use
    ``contains_(ts, item)``.

    This is logically: ``item in ts``

    To implement the contains_ operator, do:
    ::

        @compute_node(overloads=contains_)
        def my_contains(ts: TS[MyType], item: TS[SCALAR]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


@operator
def not_(ts: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the unary `not` operator for time series types.

    This must be called as ``not_(ts)`` it is not possible to overload the standard ``not`` operator.

    To implement the not_ operator, do:
    ::

        @compute_node(overloads=not_)
        def my_not(ts: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


@operator
def getitem_(ts: TIME_SERIES_TYPE, key: TS[SCALAR]) -> TIME_SERIES_TYPE_1:
    """
    This represents the `[]` operator for time-series types.

    Use this as: ``ts[key]``

    To implement the getitem_ operator, do:
    ::

        @compute_node(overloads=getitem_)
        def my_getitem(ts: TS[MyType], item: TS[SCALAR]) -> TS[SomeScalar]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__getitem__ = lambda x, y: getitem_(x, y)


@operator
def getattr_(ts: TIME_SERIES_TYPE, attr: str, default_value: SCALAR = None) -> TIME_SERIES_TYPE_1:
    """
    This represents the `.` operator for time-series types.

    Use this as: ``ts.attr`` or more explicitly: ``getattr_(ts, attr)``

    To implement the getattr_ operator, do:
    ::

        @compute_node(overloads=getattr_)
        def my_getattr(ts: TS[MyType], attr: str) -> TS[SomeScalar]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


WiringPort.__getattr__ = lambda x, y: getattr_(x, y)


@operator
def min_(*ts: TSL[TS[SCALAR], SIZE], default_value: TS[SCALAR] = None) -> TIME_SERIES_TYPE:
    """
    This represents the `min` operator for time series types.

    Unary implies the min over the latest TS value for collection types, or running min for non-collection types
    Binary or multi arg implies item-wise min over all the arguments for collection types,
    or the minimum scalar value for scalar types
    """


@operator
def max_(*ts: TSL[TS[SCALAR], SIZE], default_value: TS[SCALAR] = None) -> TIME_SERIES_TYPE:
    """
    The `max` operator for time series types.

    Unary implies the max over the latest TS value for collection types, or running max for non-collection types
    Binary or multi arg implies item-wise max over all the arguments for collection types,
    or the maximum scalar value for scalar types
    """


@operator
def sum_(*ts: TSL[TS[SCALAR], SIZE]) -> TIME_SERIES_TYPE_2:
    """
    This represents the `sum` operator for time series types, either as a binary or unary operator

    Unary implies the sum over the latest TS value for collection types, or running sum for non-collection types
    Binary or multi arg implies item-wise sum over all the arguments for collection types,
    or the sum of the scalar value for scalar types
    """


@operator
def mean(*ts: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE_2:
    """
    This represents the `mean` operator for time series types

    Unary implies the mean over the latest TS value for collection types, or running sum for non-collection types
    Binary or multi arg implies item-wise sum over all the arguments for collection types,
    or the sum of the scalar value for scalar types
    """


@operator
def std(*ts: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE_2:
    """
    Calculates the standard deviation for time series types

    Unary implies the std over the latest TS value for collection types, or running std for non-collection types
    Binary or multi arg implies item-wise std over all the arguments for collection types,
    or the std of the scalar value for scalar types
    """


@operator
def var(*ts: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE_2:
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

    To implement the len_ operator, do:
    ::

        @compute_node(overloads=len_)
        def my_len(ts: TS[MyType]) -> TS[int]:
            ...

    Then ensure that the code is imported before performing the operation.
    """


@operator
def union(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE_2:
    """
    Performs a union of the provided time-series values.

    Union is { p | p element of tsl[i] for i in range(len(tsl)) }
    """


@operator
def intersection(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE_2:
    """
    Performs an intersection of the provided time-series values.

    Intersection is { p | p in all tsl[i] for i in range(len(tsl)) }
    """


@operator
def difference(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE_2:
    """
    Performs a difference of the provided time-series values.

    Difference is { p | p element of lhs and p not element of rhs }
    """


@operator
def symmetric_difference(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE_2:
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
def accumulate(*ts: TSL[TS[SCALAR], SIZE], default_value: TS[SCALAR] = None) -> TIME_SERIES_TYPE_2:
    if default_value is not None:
        raise NotImplementedError(f"Accumulate is not implemented for {type(default_value)}")
    return sum_(*ts)


@graph(deprecated="Prefer mean")
def average(*ts: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE_2:
    return mean(*ts)
