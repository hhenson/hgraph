from typing import Tuple, Type

from hgraph import compute_node, TS, TimeSeriesSchema, TSB, SCALAR, AUTO_RESOLVE

__all__ = ('match', 'parse')


class Match(TimeSeriesSchema):
    is_match: TS[bool]
    groups: TS[Tuple[str, ...]]


@compute_node
def match(pattern: TS[str], s: TS[str]) -> TSB[Match]:
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
