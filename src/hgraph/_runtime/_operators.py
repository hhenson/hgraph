from typing import Type

from hgraph._wiring._decorators import graph, compute_node
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringError
from hgraph._wiring._wiring_port import WiringPort
from hgraph._types import TIME_SERIES_TYPE, TS, SCALAR, TIME_SERIES_TYPE_2

"""
The minimum implementation for comparisons are le_ and eq_, the remaining operators are synthesized, it is better to 
provide an actual implementation for performance reasons.
"""


__all__ = (
    "add_", "sub_", "mul_", "div_", "floordiv_", "mod_", "divmod_", "pow_", "lshift_", "rshift_", "and_", "or_", "xor_",
    "eq_", "ne_", "lt_", "le_", "gt_", "ge_", "neg_", "pos_", "abs_", "invert_", "contains_", "not_", "getitem_",
    "getattr_", "min_")


@graph
def add_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator add_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__add__ = lambda x, y: add_(x, y)
WiringPort.__radd__ = lambda x, y: add_(y, x)


@graph
def sub_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator sub_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__sub__ = lambda x, y: sub_(x, y)
WiringPort.__rsub__ = lambda x, y: sub_(y, x)


@graph
def mul_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator mul_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__mul__ = lambda x, y: mul_(x, y)
WiringPort.__rmul__ = lambda x, y: mul_(y, x)


@graph
def div_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE_2:
    raise WiringError(f"operator div_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__truediv__ = lambda x, y: div_(x, y)
WiringPort.__rtruediv__ = lambda x, y: div_(y, x)


@graph
def floordiv_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator floordiv_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__floordiv__ = lambda x, y: floordiv_(x, y)
WiringPort.__rfloordiv__ = lambda x, y: floordiv_(y, x)


@graph
def mod_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator mod_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__mod__ = lambda x, y: mod_(x, y)
WiringPort.__rmod__ = lambda x, y: mod_(y, x)


@graph
def divmod_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator divmod_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__divmod__ = lambda x, y: divmod_(x, y)
WiringPort.__rdivmod__ = lambda x, y: divmod_(y, x)


@graph
def pow_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator pow_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__pow__ = lambda x, y: pow_(x, y)
WiringPort.__rpow__ = lambda x, y: pow_(y, x)


@graph
def lshift_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator lshift_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__lshift__ = lambda x, y: lshift_(x, y)
WiringPort.__rlshift__ = lambda x, y: lshift_(y, x)


@graph
def rshift_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator rshift_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__rshift__ = lambda x, y: rshift_(x, y)
WiringPort.__rrshift__ = lambda x, y: rshift_(y, x)


@compute_node
def and_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    return bool(lhs.value and rhs.value)


WiringPort.__and__ = lambda x, y: and_(x, y)
WiringPort.__rand__ = lambda x, y: and_(y, x)


@compute_node
def or_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    return bool(lhs.value or rhs.value)


WiringPort.__or__ = lambda x, y: or_(x, y)
WiringPort.__ror__ = lambda x, y: or_(y, x)


@compute_node
def xor_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    return lhs.value ^ rhs.value


WiringPort.__xor__ = lambda x, y: xor_(x, y)
WiringPort.__rxor__ = lambda x, y: xor_(y, x)


@compute_node
def eq_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    return bool(lhs.value == rhs.value)


# This is currently safe to do as the wiring port needs to be immutable, but is never used as a key in a dict or
# compared to another port. But in case we need access to the original store it back on the class.
WiringPort.__orig_eq__ = WiringPort.__eq__
WiringPort.__eq__ = lambda x, y: eq_(x, y)


@graph
def ne_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    from hgraph.nodes import not_
    return not_(lhs == rhs)


WiringPort.__ne__ = lambda x, y: ne_(x, y)


@graph
def lt_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    raise WiringError(f"operator lt_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__lt__ = lambda x, y: lt_(x, y)


@graph
def le_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    return or_(lhs < rhs, lhs == rhs)


WiringPort.__le__ = lambda x, y: le_(x, y)


@graph
def gt_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    return not_(le_(lhs, rhs))


WiringPort.__gt__ = lambda x, y: gt_(x, y)


@graph
def ge_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    return not_(lt_(lhs, rhs))


WiringPort.__ge__ = lambda x, y: ge_(x, y)


@graph
def neg_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator neg_ is not implemented for {ts.output_type}")


WiringPort.__neg__ = lambda x: neg_(x)


@graph
def pos_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator pos_ is not implemented for {ts.output_type}")


WiringPort.__pos__ = lambda x: pos_(x)


@compute_node
def abs_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    return abs(ts.value)


WiringPort.__abs__ = lambda x: abs_(x)


@graph
def invert_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator invert_ is not implemented for {ts.output_type}")


WiringPort.__invert__ = lambda x: invert_(x)


@compute_node
def contains_(ts: TIME_SERIES_TYPE, item: TS[SCALAR]) -> TS[bool]:
    return bool(item.value in ts.value)


# Can't override __contains__ as it seems to always returns a bool value.
# WiringPort.__contains__ = lambda x, y: contains_(x, y)


@compute_node
def not_(ts: TIME_SERIES_TYPE) -> TS[bool]:
    """logic not"""
    return bool(not ts.value)


@graph
def getitem_(ts: TIME_SERIES_TYPE, key: TS[SCALAR]) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator getitem_ is not implemented for {ts.output_type} and {key.output_type}")


WiringPort.__getitem__ = lambda x, y: getitem_(x, y)


@graph
def getattr_(ts: TIME_SERIES_TYPE, attr: str) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator getattr_ is not implemented for {ts.output_type} and {attr}")


WiringPort.__getattr__ = lambda x, y: getattr_(x, y)


@graph
def min_(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator min_ is not implemented for {ts.output_type}")
