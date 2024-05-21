"""
The home of hgraph operator overloads.
"""
from hgraph import compute_node, len_, TS, SCALAR, contains_, SCALAR_1, eq_, ne_, not_, abs_, and_, or_, mod_, min_op, \
    max_op

__all__ = (
"len_ts", "contains_ts", "eq_ts", "ne_ts", "not_ts", "abs_ts", "and_ts", "or_ts", "mod_ts", "min_op", "max_op")


@compute_node(overloads=len_)
def len_ts(ts: TS[SCALAR]) -> TS[int]:
    """
    Returns the notion of length for the input time-series.
    By default, it is the length of the value of the time-series.
    """
    return len(ts.value)


@compute_node(overloads=contains_)
def contains_ts(ts: TS[SCALAR], key: TS[SCALAR_1]) -> TS[bool]:
    """Implements using the standard ``in`` Python operator"""
    return key.value in ts.value


@compute_node(overloads=eq_)
def eq_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """Implements using the standard ``==`` Python operator"""
    return bool(lhs.value == rhs.value)


@compute_node(overloads=ne_)
def ne_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """Implements using the standard ``!=`` Python operator"""
    return bool(lhs.value != rhs.value)


@compute_node(overloads=not_)
def not_ts(ts: TS[SCALAR]) -> TS[bool]:
    """Implements not_ using the standard Python ``not`` operator"""
    return not ts.value


@compute_node(overloads=abs_)
def abs_ts(ts: TS[SCALAR]) -> TS[SCALAR]:
    """Implements using the standard ``abs`` Python operator"""
    return abs(ts.value)


@compute_node(overloads=and_)
def and_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """Implements using the standard ``and`` Python operator"""
    return bool(lhs.value and rhs.value)


@compute_node(overloads=or_)
def or_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """Implements using the standard ``or`` Python operator"""
    return bool(lhs.value or rhs.value)


@compute_node(overloads=mod_)
def mod_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """Implements using the standard ``mod`` Python operator"""
    return lhs.value % rhs.value


@compute_node(overloads=min_op)
def min_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """Implements using the standard ``min`` Python operator"""
    return min(lhs.value, rhs.value)


@compute_node(overloads=max_op)
def max_ts(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """Implements using the standard ``max`` Python operator"""
    return max(lhs.value, rhs.value)
