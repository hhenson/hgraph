"""
Operators to facilitate pair manipulation
"""
from hgraph.arrow import arrow
from hgraph.arrow._arrow import A, _make_tuple, B

__all__ = ("first", "swap", "second", "assoc")

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
    return _make_tuple(pair[1], pair[0])


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
    return _make_tuple(pair[0][0], _make_tuple(pair[0][1], pair[1]))
