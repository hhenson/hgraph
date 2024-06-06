from typing import Tuple, Type

from hgraph import compute_node, combine, TSL, TIME_SERIES_TYPE, SIZE, TS, SCALAR, DEFAULT, OUT

__all__ = ("combine_tuple",)


@compute_node(overloads=combine, requires=lambda m, s: m[OUT].py_type == TS[Tuple], all_valid=('tsl',))
def combine_tuple(*tsl: TSL[TS[SCALAR], SIZE]) -> TS[Tuple[SCALAR, ...]]:
    return tuple(v.value for v in tsl)


@compute_node(overloads=combine, requires=lambda m, s: m[OUT].py_type == TS[Tuple] and s['strict'] is False)
def combine_tuple_relaxed(*tsl: TSL[TS[SCALAR], SIZE], strict: bool) -> TS[Tuple[SCALAR, ...]]:
    return tuple(v.value for v in tsl)
