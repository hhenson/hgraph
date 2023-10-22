from typing import TypeVar

from hg import compute_node, TS

__all__ = ("NUMBER", "add_")


NUMBER = TypeVar("NUMBER", int, float)


@compute_node
def add_(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[NUMBER]:
    """ Adds two time-series values of numbers together"""
    return lhs.value + rhs.value
