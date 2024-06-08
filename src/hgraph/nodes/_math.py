import sys
from typing import Type

from hgraph import TS, graph, WiringNodeClass, zero

__all__ = ("zero_int", "zero_float")


@graph(overloads=zero)
def zero_int(tp: Type[TS[int]], op: WiringNodeClass) -> TS[int]:
    mapping = {
        'add_': 0,
        'sum_': 0,
        'min_': sys.maxsize,
        'max_': -sys.maxsize,
        'mul_': 1,
    }
    return mapping[op.signature.name]


@graph(overloads=zero)
def zero_float(tp: Type[TS[float]], op: WiringNodeClass) -> TS[float]:
    mapping = {
        'add_': 0.0,
        'sum_': 0.0,
        'min_': float('inf'),
        'max_': -float('inf'),
        'mul_': 1.0,
    }
    return mapping[op.signature.name]
