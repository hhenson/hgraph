from typing import Tuple, Type

from hgraph import compute_node, TS, TimeSeriesSchema, TSB, SCALAR, AUTO_RESOLVE, add_, TIME_SERIES_TYPE, str_, sub_, \
    WiringError, graph, mul_

__all__ = ('match_', 'parse', 'add_str')


@compute_node(overloads=str_)
def str_(ts: TIME_SERIES_TYPE) -> TS[str]:
    """
    Returns the string representation of the time-series value.
    """
    return str(ts.value)


@compute_node(overloads=add_)
def add_str(lhs: TS[str], rhs: TS[str]) -> TS[str]:
    """
    Concatenates two strings.
    """
    return lhs.value + rhs.value


class Match(TimeSeriesSchema):
    is_match: TS[bool]
    groups: TS[Tuple[str, ...]]


@compute_node
def match_(pattern: TS[str], s: TS[str]) -> TSB[Match]:
    """
    Matches the pattern in the string and returns the groups.
    """
    import re
    m = re.match(pattern.value, s.value)
    if m:
        return {'is_match': True, 'groups': m.groups()}
    else:
        return {'is_match': False}


@compute_node
def parse(s: TS[str], tp: Type[SCALAR] = AUTO_RESOLVE) -> TS[SCALAR]:
    """
    Parses the string into the given type.
    """
    return tp(s.value)


@graph(overloads=sub_)
def sub_strs(lhs: TS[str], rhs: TS[str]) -> TS[str]:
    raise WiringError("Cannot subtract one string from another")


@compute_node(overloads=mul_)
def mul_strs(lhs: TS[str], rhs: TS[int]) -> TS[str]:
    return lhs.value * rhs.value


@graph
def str_str(ts: TS[str]) -> TS[str]:
    return ts
