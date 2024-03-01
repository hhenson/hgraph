"""
Creating a graph from stateful nodes allows for recovery of a computation or replay of a computational graph with changes.
"""
from typing import Callable

from hgraph import pull_source_node, TS, compute_node, STATE, CompoundScalar, graph


def stateful(fn: Callable, *args, **kwargs) -> Callable:
    ...


def stateful_node(fn: Callable, *args):
    ...


class EwmaSchema(CompoundScalar):
    s: float
    count: int


@compute_node
def ewma(ts: TS[float], alpha: float, window_size: int, _state: STATE[EwmaSchema]) -> TS[float]:
    ...


@graph
def compute_a_signal() -> TS[float]:
    input: TS[float] = ...
    w = window[store: "result.window.1"](input, window_size=100)
    result = ewma[store: "result.ewma"](input, 1.0, 20)
