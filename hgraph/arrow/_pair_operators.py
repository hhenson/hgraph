"""
Operators to facilitate pair manipulation
"""

from hgraph import TSL, OUT, SIZE, TSB, ts_schema
from hgraph.arrow import arrow
from hgraph.arrow._arrow import A, make_pair, B, Pair, _flatten

__all__ = ("first", "swap", "second", "assoc", "flatten_tsl", "to_pair", "flatten_tsb")


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
    """
    Flattens a pair into a TSL. This requires each element of the pair to have the same type.
    """
    v = _flatten(x)
    tp = v[0].output_type.py_type
    if not all(tp == i.output_type.py_type for i in v):
        raise ValueError(
            f"All elements must have the same type, got types: ({','.join(str(i.output_type) for i in v)})"
        )
    return TSL.from_ts(*v)


def flatten_tsb(__schema__=None, **kwargs):
    """
    Convert the input in the TSB schema, this flattens the pairs and then attempts to assign them to the schema
    in the order specified.

    This can be called with a schema or a dictionary of types or a kwargs of types.
    """
    if __schema__ is not None:
        schema = __schema__
    else:
        schema = kwargs
    if isinstance(schema, dict):
        schema = ts_schema(**schema)

    def _wrapper(x):
        v = _flatten(x)
        if len(v) != len(schema.__meta_data_schema__):
            raise ValueError(
                f"Expected {len(schema.__meta_data_schema__)} values, got {len(v)} for schema: "
                f"{dict({k: str(v) for k, v in schema.__meta_data_schema__.items()})}"
            )
        return TSB[schema].from_ts(**{k: v for k, v in zip(schema.__meta_data_schema__.keys(), v)})

    return arrow(_wrapper)


def to_pair(first_: str | int, second_: str | int) -> TSB[Pair[A, B]]:
    """
    Converts a collection type that can be referenced to a pair using a named lookup (for example: TSB, TSD).
    Or indexed lookup (for example: TSL)
    """

    def _wrapper(x):
        return make_pair(x[first_], x[second_])

    return arrow(_wrapper)
