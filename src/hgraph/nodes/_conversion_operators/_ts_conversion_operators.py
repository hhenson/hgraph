from collections import deque
from dataclasses import dataclass, field
from typing import Mapping, Set, Tuple

from frozendict import frozendict

from hgraph import compute_node, TS, SCALAR, STATE, CompoundScalar, SCHEDULER, MIN_TD, SCALAR_1, graph, \
    AUTO_RESOLVE, TSS, OUT
from hgraph._types._scalar_types import DEFAULT
from hgraph._operators._conversion import emit, convert

__all__ = ()


@compute_node(overloads=convert, requires=lambda m, s: m[SCALAR] != m[SCALAR_1] and m[SCALAR_1].py_type not in (tuple, Tuple, set, frozenset, Set, dict, frozendict, Mapping))
def convert_ts_scalar(ts: TS[SCALAR], to: type[TS[SCALAR_1]] = DEFAULT[OUT], s1_type: type[SCALAR_1] = AUTO_RESOLVE) -> TS[SCALAR_1]:
    return s1_type(ts.value)
