from hgraph._operators._operators import min_, max_, eq_, lt_, gt_, le_, ge_, getattr_
from hgraph._types._scalar_types import ENUM
from hgraph._types._ts_type import TS, TS_OUT
from hgraph._types._tsl_type import TSL, SIZE
from hgraph._wiring._decorators import graph, compute_node

__all__ = tuple()


@graph(overloads=min_)
def min_enum(*ts: TSL[TS[ENUM], SIZE], default_value: TS[ENUM] = None, __strict__: bool = True) -> TS[ENUM]:
    if len(ts) == 1:
        return min_enum_unary(ts[0])
    elif len(ts) == 2:
        return min_enum_binary(ts[0], ts[1], __strict__)
    else:
        return min_enum_multi(*ts, default_value=default_value, __strict__=__strict__)


@compute_node
def min_enum_unary(ts: TS[ENUM], _output: TS_OUT[ENUM] = None) -> TS[ENUM]:
    """
    Unary min() - running min
    """
    if not _output.valid:
        return ts.value
    elif ts.value.value < _output.value.value:
        return ts.value


@compute_node(valid=lambda m, s: ("lhs", "rhs") if s["__strict__"] else ())
def min_enum_binary(lhs: TS[ENUM], rhs: TS[ENUM], __strict__: bool = True) -> TS[ENUM]:
    if lhs.valid and rhs.valid:
        lhs_val, rhs_val = lhs.value, rhs.value
        return lhs_val if lhs_val.value <= rhs_val.value else rhs_val
    if lhs.valid:
        return lhs.value
    if rhs.valid:
        return rhs.value


@compute_node(all_valid=lambda m, s: ("ts",) if s["__strict__"] else None)
def min_enum_multi(*ts: TSL[TS[ENUM], SIZE], default_value: TS[ENUM] = None, __strict__: bool = True) -> TS[ENUM]:
    """
    Multi-arg enum value min()
    """
    return min((arg.value for arg in ts if arg.valid), key=lambda enum: enum.value, default=default_value.value)


@graph(overloads=max_)
def max_enum(*ts: TSL[TS[ENUM], SIZE], default_value: TS[ENUM] = None, __strict__: bool = True) -> TS[ENUM]:
    if len(ts) == 1:
        return max_enum_unary(ts[0])
    elif len(ts) == 2:
        return max_enum_binary(ts[0], ts[1], __strict__)
    else:
        return max_enum_multi(*ts, default_value=default_value, __strict__=__strict__)


@compute_node
def max_enum_unary(ts: TS[ENUM], _output: TS_OUT[ENUM] = None) -> TS[ENUM]:
    """
    Unary max() - running max
    """
    if not _output.valid:
        return ts.value
    elif ts.value.value > _output.value.value:
        return ts.value


@compute_node(valid=lambda m, s: ("lhs", "rhs") if s["__strict__"] else ())
def max_enum_binary(lhs: TS[ENUM], rhs: TS[ENUM], __strict__: bool = True) -> TS[ENUM]:
    if lhs.valid and rhs.valid:
        lhs_val, rhs_val = lhs.value, rhs.value
        return lhs_val if lhs_val.value >= rhs_val.value else rhs_val
    if lhs.valid:
        return lhs.value
    if rhs.valid:
        return rhs.value


@compute_node(all_valid=lambda m, s: ("ts",) if s["__strict__"] else None)
def max_enum_multi(*ts: TSL[TS[ENUM], SIZE], default_value: TS[ENUM] = None, __strict__: bool = True) -> TS[ENUM]:
    """
    Multi-arg enum value max()
    """
    return max((arg.value for arg in ts if arg.valid), key=lambda enum: enum.value, default=default_value.value)


@compute_node(overloads=eq_)
def eq_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return lhs.value is rhs.value


@compute_node(overloads=lt_)
def lt_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return bool(lhs.value.value < rhs.value.value)


@compute_node(overloads=gt_)
def gt_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return bool(lhs.value.value > rhs.value.value)


@compute_node(overloads=le_)
def le_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return bool(lhs.value.value <= rhs.value.value)


@compute_node(overloads=ge_)
def ge_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return bool(lhs.value.value >= rhs.value.value)


@compute_node(overloads=getattr_)
def getattr_enum_name(ts: TS[ENUM], attribute: str) -> TS[str]:
    if attribute == "name":
        return ts.value.name
    else:
        raise AttributeError(f"Cannot get {attribute} from TS[Enum]")
