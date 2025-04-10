"""
Operators to facilitate pair manipulation
"""

from hgraph import TSL, OUT, SIZE
from hgraph.arrow import arrow
from hgraph.arrow._arrow import A, make_pair, B, Pair, _flatten

__all__ = ("first", "swap", "second", "assoc", "flatten_tsl")


@arrow
def first(pair) -> A:
    """
    Returns the first element of a tuple
    """
    return pair[0]


@arrow
def swap(pair):
    """
    Swaps the values in a tuple.
    """
    return make_pair(pair[1], pair[0])


@arrow
def second(pair) -> B:
    """Returns the second element of a tuple"""
    return pair[1]


@arrow
def assoc(pair):
    """
    Adjust the associativity of a pair.
    Converts ((a, b), c) -> (a, (b, c)).
    """
    return make_pair(pair[0][0], make_pair(pair[0][1], pair[1]))


@arrow
def flatten_tsl(x: Pair[A, B]) -> TSL[OUT, SIZE]:
    v = _flatten(x)
    tp = v[0].output_type.py_type
    if not all(tp == i.output_type.py_type for i in v):
        raise ValueError(
            f"All elements must have the same type, got types: ({','.join(str(i.output_type) for i in v)})")
    return TSL.from_ts(*v)

