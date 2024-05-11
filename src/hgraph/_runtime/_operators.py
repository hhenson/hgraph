from typing import Type

from hgraph._types import TIME_SERIES_TYPE, TS, SCALAR, TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2
from hgraph._types._scalar_types import Size, SIZE
from hgraph._types._tsl_type import TSL
from hgraph._wiring._decorators import graph
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringError, WiringNodeClass
from hgraph._wiring._wiring_port import WiringPort

"""
The minimum implementation for comparisons are le_ and eq_, the remaining operators are synthesized, it is better to 
provide an actual implementation for performance reasons.
"""

__all__ = (
    "add_", "sub_", "mul_", "div_", "floordiv_", "mod_", "divmod_", "pow_", "lshift_", "rshift_", "and_", "or_", "xor_",
    "eq_", "ne_", "lt_", "le_", "gt_", "ge_", "neg_", "pos_", "abs_", "invert_", "contains_", "not_", "getitem_",
    "getattr_", "min_", "max_", "zero", "len_", "min_op", "max_op", "and_op", "or_op", "union_op", "union", "union_tsl",
    "intersection_op", "intersection", "intersection_tsl", "difference", "symmetric_difference", "is_empty", "type_"
)


@graph
def add_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> TIME_SERIES_TYPE_2:
    """
    This represents the `+` operator for time series types.
    This is the interface definition graph, by default it is not implemented.
    To implement the add_ operator, do:
    ::

        @compute_node(overloads=add_)
        def my_add(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator add_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__add__ = lambda x, y: add_(x, y)
WiringPort.__radd__ = lambda x, y: add_(y, x)


@graph
def sub_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> TIME_SERIES_TYPE_2:
    """
    This represents the `-` operator for time series types.
    This is the interface definition graph, by default it is not implemented.
    To implement the sub_ operator, do:
    ::

        @compute_node(overloads=sub_)
        def my_sub(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator sub_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__sub__ = lambda x, y: sub_(x, y)
WiringPort.__rsub__ = lambda x, y: sub_(y, x)


@graph
def mul_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `*` operator for time series types.
    This is the interface definition graph, by default it is not implemented.
    To implement the mul_ operator, do:
    ::

        @compute_node(overloads=mul_)
        def my_mul(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator mul_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__mul__ = lambda x, y: mul_(x, y)
WiringPort.__rmul__ = lambda x, y: mul_(y, x)


@graph
def div_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE_2:
    """
    This represents the `/` operator for time series types.
    This is the interface definition graph, by default it is not implemented.
    To implement the div_ operator, do:
    ::

        @compute_node(overloads=div_)
        def my_div(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator div_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__truediv__ = lambda x, y: div_(x, y)
WiringPort.__rtruediv__ = lambda x, y: div_(y, x)


@graph
def floordiv_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `//` operator for time series types.
    This is the interface definition graph, by default it is not implemented.
    To implement the floordiv_ operator, do:
    ::

        @compute_node(overloads=floordiv_)
        def my_floordiv(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator floordiv_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__floordiv__ = lambda x, y: floordiv_(x, y)
WiringPort.__rfloordiv__ = lambda x, y: floordiv_(y, x)


@graph
def mod_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `%` operator for time series types.
    This is the interface definition graph, by default it is not implemented.
    To implement the mod_ operator, do:
    ::

        @compute_node(overloads=mod_)
        def my_mod(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator mod_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__mod__ = lambda x, y: mod_(x, y)
WiringPort.__rmod__ = lambda x, y: mod_(y, x)


@graph
def divmod_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TSL[TIME_SERIES_TYPE, Size[2]]:
    """
    This represents the `divmod` operator for time series types.
    (This is defined in Python as the integer division with remainder, i.e. divmod(9, 4) == (2, 1))
    This is the interface definition graph, by default it is not implemented.
    To implement the divmod_ operator, do:
    ::

        @compute_node(overloads=divmod_)
        def my_divmod(lhs: TS[MyType], rhs: TS[MyType]) -> TSL[TS[MyType], Size[2]]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator divmod_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__divmod__ = lambda x, y: divmod_(x, y)
WiringPort.__rdivmod__ = lambda x, y: divmod_(y, x)


@graph
def pow_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `**` operator for time series types.
    This is the interface definition graph, by default it is not implemented.
    To implement the pow_ operator, do:
    ::

        @compute_node(overloads=pow_)
        def my_pow(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator pow_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__pow__ = lambda x, y: pow_(x, y)
WiringPort.__rpow__ = lambda x, y: pow_(y, x)


@graph
def lshift_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `<<` operator for time series types.
    This is the interface definition graph, by default it is not implemented.
    To implement the lshift_ operator, do:
    ::

        @compute_node(overloads=lshift_)
        def my_lshift(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator lshift_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__lshift__ = lambda x, y: lshift_(x, y)
WiringPort.__rlshift__ = lambda x, y: lshift_(y, x)


@graph
def rshift_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `>>` operator for time series types.
    This is the interface definition graph, by default it is not implemented.
    To implement the rshift_ operator, do:
    ::

        @compute_node(overloads=rshift_)
        def my_rshift(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator rshift_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__rshift__ = lambda x, y: rshift_(x, y)
WiringPort.__rrshift__ = lambda x, y: rshift_(y, x)


@graph
def and_op(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[TIME_SERIES_TYPE_1]:
    """
    This represents the `&` operator for time series types.

    The default implementation of this is to use ``and_(lhs, rhs)``.

    This is the interface definition graph, by default it is not implemented.
    To implement the and_op_ operator, do:
    ::

        @compute_node(overloads=and_op_)
        def my_and_op(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    return and_(lhs, rhs)


WiringPort.__and__ = lambda x, y: and_op(x, y)
WiringPort.__rand__ = lambda x, y: and_op(y, x)


@graph
def and_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `and` operator for time series types.

    This operator does not substitute ``and`` (since that is not possible in Python), but can be used as a functional
    equivalent for ``and``.

    This is the interface definition graph, by default it is not implemented.
    To implement the and_ operator, do:
    ::

        @compute_node(overloads=and_)
        def my_and(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator and_ is not implemented for {lhs.output_type} and {rhs.output_type}")


@graph
def or_op(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[TIME_SERIES_TYPE_1]:
    """
    This represents the `|` operator for time series types.

    By default, this returns ``or_(lhs, rhs)``.

    This is the interface definition graph, by default it is not implemented.
    To implement the or_ operator, do:
    ::

        @compute_node(overloads=or_)
        def my_or(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    return or_(lhs, rhs)


WiringPort.__or__ = lambda x, y: or_op(x, y)
WiringPort.__ror__ = lambda x, y: or_op(y, x)


@graph
def or_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `or` operator for time series types.

    This operator does not substitute ``or`` (since that is not possible in Python), but can be used as a functional
    equivalent for ``or``.

    This is the interface definition graph, by default it is not implemented.
    To implement the or_ operator, do:
    ::

        @compute_node(overloads=or_)
        def my_or(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator or_ is not implemented for {lhs.output_type} and {rhs.output_type}")


@graph
def xor_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE_1:
    """
    This represents the `^` operator for time series types.

    This is the interface definition graph, by default it is not implemented.
    To implement the xor_ operator, do:
    ::

        @compute_node(overloads=xor_)
        def my_xor(lhs: TS[MyType], rhs: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator or_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__xor__ = lambda x, y: xor_(x, y)
WiringPort.__rxor__ = lambda x, y: xor_(y, x)


@graph
def eq_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `==` operator for time series types.

    This is the interface definition graph, by default it is not implemented.
    To implement the eq_ operator, do:
    ::

        @compute_node(overloads=eq_)
        def my_eq(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator eq_ is not implemented for {lhs.output_type} and {rhs.output_type}")


# This is currently safe to do as the wiring port needs to be immutable, but is never used as a key in a dict or
# compared to another port. But in case we need access to the original store it back on the class.
WiringPort.__orig_eq__ = WiringPort.__eq__
WiringPort.__eq__ = lambda x, y: eq_(x, y)


@graph
def ne_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `!=` operator for time series types.

    By default, this returns ``not_(eq_(lhs, rhs))``.

    This is the interface definition graph, by default it is not implemented.
    To implement the ne_ operator, do:
    ::

        @compute_node(overloads=ne_)
        def my_ne(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    return not_(eq_(lhs, rhs))


WiringPort.__ne__ = lambda x, y: ne_(x, y)


@graph
def lt_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `<` operator for time series types.
    This is the interface definition graph, by default it is not implemented.
    To implement the lt_ operator, do:
    ::

        @compute_node(overloads=lt_)
        def my_lt(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator lt_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__lt__ = lambda x, y: lt_(x, y)


@graph
def le_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `<=` operator for time series types.

    The default implementation is ``or_(le_(lhs, rhs), eq_(lhs, rhs))``.

    This is the interface definition graph, by default it is not implemented.
    To implement the le_ operator, do:
    ::

        @compute_node(overloads=le_)
        def my_le(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    return or_(lt_(lhs, rhs), eq_(lhs, rhs))


WiringPort.__le__ = lambda x, y: le_(x, y)


@graph
def gt_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `>` operator for time series types.

    The default implementation is ``not_(le_(lhs, rhs))``.

    This is the interface definition graph, by default it is not implemented.
    To implement the gt_ operator, do:
    ::

        @compute_node(overloads=gt_)
        def my_gt(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    return not_(le_(lhs, rhs))


WiringPort.__gt__ = lambda x, y: gt_(x, y)


@graph
def ge_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the `>=` operator for time series types.

    The default implementation is ``not_(lt_(lhs, rhs))``.

    This is the interface definition graph, by default it is not implemented.
    To implement the ge_ operator, do:
    ::

        @compute_node(overloads=ge_)
        def my_ge(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    return not_(lt_(lhs, rhs))


WiringPort.__ge__ = lambda x, y: ge_(x, y)


@graph
def neg_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the unary `-` operator for time series types.

    The default implementation is ``sub_(zero(ts.signature.output_type.py_type, sub_), ts)``.

    This is the interface definition graph, by default it is not implemented.
    To implement the neg_ operator, do:
    ::

        @compute_node(overloads=neg_)
        def my_neg(ts: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    ts: WiringNodeClass  # In a graph this is the actual underlying type
    return sub_(zero(ts.signature.output_type.py_type, sub_), ts)


WiringPort.__neg__ = lambda x: neg_(x)


@graph
def pos_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the unary `+` operator for time series types.

    The default implementation is ``add_(zero(ts.signature.output_type.py_type, add_), ts)``.

    This is the interface definition graph, by default it is not implemented.
    To implement the pos_ operator, do:
    ::

        @compute_node(overloads=pos_)
        def my_pos(ts: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    ts: WiringNodeClass  # In a graph this is the actual underlying type
    return add_(zero(ts.signature.output_type.py_type, add_), ts)


WiringPort.__pos__ = lambda x: pos_(x)


@graph
def abs_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `abs` operator for time series types.

    This is the interface definition graph, by default it is not implemented.
    To implement the abs_ operator, do:
    ::

        @compute_node(overloads=abs_)
        def my_abs(ts: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator abs_ is not implemented for {ts.output_type}")


WiringPort.__abs__ = lambda x: abs_(x)


@graph
def invert_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the unary `~` operator for time series types.

    This is the interface definition graph, by default it is not implemented.
    To implement the pos_ operator, do:
    ::

        @compute_node(overloads=pos_)
        def my_pos(ts: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator abs_ is not implemented for {ts.output_type}")


WiringPort.__invert__ = lambda x: invert_(x)


@graph
def contains_(ts: TIME_SERIES_TYPE, item: TS[SCALAR]) -> TS[bool]:
    """
    This represents the `in` operator for time series types, however, since ``__contains__`` always returns a bool
    value, we can't overload the __contains__, so it is not possible to do ``item in ts``, instead use
    ``contains_(ts, item)``.

    This is logically: ``item in ts``

    This is the interface definition graph, by default it is not implemented.
    To implement the contains_ operator, do:
    ::

        @compute_node(overloads=contains_)
        def my_contains(ts: TS[MyType], item: TS[SCALAR]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator contains_ is not implemented for {ts.output_type} and {item.output_type}")


# Can't override __contains__ as it seems to always returns a bool value.
# WiringPort.__contains__ = lambda x, y: contains_(x, y)


@graph
def not_(ts: TIME_SERIES_TYPE) -> TS[bool]:
    """
    This represents the unary `not` operator for time series types.

    This must be called as ``not_(ts)`` it is not possible to overload the standard ``not`` operator.

    This is the interface definition graph, by default it is not implemented.
    To implement the not_ operator, do:
    ::

        @compute_node(overloads=not_)
        def my_not(ts: TS[MyType]) -> TS[MyType]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator not_ is not implemented for {ts.output_type}")


@graph
def getitem_(ts: TIME_SERIES_TYPE, key: TS[SCALAR]) -> TIME_SERIES_TYPE_1:
    """
    This represents the `[]` operator for time-series types.

    Use this as: ``ts[key]``

    This is the interface definition graph, by default it is not implemented.
    To implement the getitem_ operator, do:
    ::

        @compute_node(overloads=getitem_)
        def my_getitem(ts: TS[MyType], item: TS[SCALAR]) -> TS[SomeScalar]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator getitem_ is not implemented for {ts.output_type} and {key.output_type}")


WiringPort.__getitem__ = lambda x, y: getitem_(x, y)


@graph
def getattr_(ts: TIME_SERIES_TYPE, attr: str) -> TIME_SERIES_TYPE_1:
    """
    This represents the `.` operator for time-series types.

    Use this as: ``ts.attr`` or more explicitly: ``getattr(ts, attr)``

    This is the interface definition graph, by default it is not implemented.
    To implement the getattr_ operator, do:
    ::

        @compute_node(overloads=getattr_)
        def my_getattr(ts: TS[MyType], attr: str) -> TS[SomeScalar]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator getattr_ is not implemented for {ts.output_type} and {attr}")


WiringPort.__getattr__ = lambda x, y: getattr_(x, y)


@graph
def min_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
    if rhs is None:
        return lhs
    else:
        return min_op(lhs, rhs)


@graph
def min_op(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `min` operator for time series types.

    This is expected to return the minimum value of the two provided time-series values.

    This is the interface definition graph, by default it is not implemented.
    To implement the min_op_ operator, do:
    ::

        @compute_node(overloads=min_op_)
        def my_min_op(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator min_ is not implemented for {lhs.output_type}")


@graph
def max_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
    if rhs is None:
        return lhs
    else:
        return max_op(lhs, rhs)


@graph
def max_op(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    This represents the `max` operator for time series types.

    This is expected to return the maximum value of the two provided time-series values.

    This is the interface definition graph, by default it is not implemented.
    To implement the max_op_ operator, do:
    ::

        @compute_node(overloads=max_op_)
        def my_max_op(lhs: TS[MyType], rhs: TS[MyType]) -> TS[bool]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator max_ is not implemented for {lhs.output_type}")


@graph
def zero(tp: Type[TIME_SERIES_TYPE], op: WiringNodeClass) -> TIME_SERIES_TYPE_2:
    """
    This is a helper graph to create a zero time-series for the reduce function. The zero values are
    type nad operation dependent so both are provided. The datatype designers should overload this graph for their
    respective data types and return correct zero values for the operation.
    """
    raise WiringError(f"operator zero is not implemented for {tp} and operation {op.signature.name}")


@graph
def len_(ts: TIME_SERIES_TYPE) -> TS[int]:
    """
    This represents the `len` operator for time series types.

    This is the interface definition graph, by default it is not implemented.
    To implement the len_ operator, do:
    ::

        @compute_node(overloads=len_)
        def my_len(ts: TS[MyType]) -> TS[int]:
            ...

    Then ensure that the code is imported before performing the operation.
    """
    raise WiringError(f"operator len_ is not implemented for {ts.output_type}")


# SET Operators

def union(*args: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Performs a union of a collection of time-series values.
    Each arg must be of the same type.
    """
    if len(args) == 1:
        return args[0]

    return union_tsl(TSL.from_ts(*args))


@graph
def union_tsl(tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE:
    """
    Performs a union of the provided time-series values.

    By default, is ``reduce(union_op, tls)``

    Union is { p | p element of tsl[i] for i in range(len(tsl)) }
    """
    from hgraph._wiring._reduce import reduce
    raise reduce(union_op, tsl)


@graph
def union_op(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Performs a union of the provided time-series values.

    Union is { p | p element of lhs and p element of rhs }
    """
    raise WiringError(
        f"operator union_op is not implemented for {lhs.output_type} and {rhs.output_type}")


def intersection(*args: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Performs a union of a collection of time-series values.
    Each arg must be of the same type.
    """
    if len(args) == 1:
        return args[0]

    return intersection_tsl(TSL.from_ts(*args))


@graph
def intersection_tsl(tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE:
    """
    Performs an intersection of the provided time-series values.

    Intersection is { p | p in all tsl[i] for i in range(len(tsl)) }
    """
    from hgraph._wiring._reduce import reduce
    raise reduce(intersection_op, tsl)


@graph
def intersection_op(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Performs an intersection of the provided time-series values.

    Intersection is { p | p in lhs and p in rhs }
    """
    raise WiringError(
        f"operator union_op is not implemented for {lhs.output_type} and {rhs.output_type}")


@graph
def difference(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Performs a difference of the provided time-series values.

    Difference is { p | p element of lhs and p not element of rhs }
    """
    raise WiringError(f"operator difference is not implemented for {lhs.output_type}")


@graph
def symmetric_difference(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Performs the symmetric difference of the provided time-series values.

    Symmetric difference is { p | p element of union(lhs, rhs), but not element of intersection(lhs, rhs) }
    """
    raise WiringError(f"operator symmetric_difference is not implemented for {lhs.output_type}")


@graph
def is_empty(ts: TIME_SERIES_TYPE) -> TS[bool]:
    """
    Returns True if the value of the time-series is considered empty, False otherwise.

    By default
    """
    return eq_(len_(ts), 0)


@graph
def type_(ts: TIME_SERIES_TYPE) -> TS[type]:
    """
    Returns the type of the time-series value.
    """
    return type(ts.value)
