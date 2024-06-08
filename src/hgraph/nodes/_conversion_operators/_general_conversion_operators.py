from typing import Type

from hgraph import OUT, DEFAULT, graph, convert

__all__ = ()


@graph(overloads=convert)
def convert_noop(ts: OUT, to: Type[OUT] = OUT) -> DEFAULT[OUT]:
    """
    if types are the same, then return the value.
    """
    return ts
