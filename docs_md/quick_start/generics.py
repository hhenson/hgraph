from typing import TypeVar, Mapping

from frozendict import frozendict

from hgraph import compute_node, TS, graph, evaluate_graph, GraphConfiguration, SCALAR, TIME_SERIES_TYPE, Size, TSL, TSD, debug_print

NUMERIC = TypeVar("NUMERIC", int, float)


@compute_node
def add(a: TS[NUMERIC], b: TS[NUMERIC]) -> TS[NUMERIC]:
    return a.value + b.value


@graph
def main():
    debug_print("1+2", add(a=1, b=2))


evaluate_graph(main, GraphConfiguration())

### Example two


@compute_node
def cast(value: TS[SCALAR]) -> TIME_SERIES_TYPE:
    return value.value


@graph
def main():
    debug_print("TS[Mapping[int, str]]", cast[TIME_SERIES_TYPE : TS[Mapping[int, str]]](value=frozendict({1: "a"})))
    debug_print("TSL[TS[str], Size[2]]", cast[TIME_SERIES_TYPE : TSL[TS[str], Size[2]]](value=frozendict({1: "a"})))
    debug_print("TSD[int, TS[str]]", cast[TIME_SERIES_TYPE : TSD[int, TS[str]]](value=frozendict({1: "a"})))


evaluate_graph(main, GraphConfiguration())
