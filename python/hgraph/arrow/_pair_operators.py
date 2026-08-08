"""Pair manipulation operators — port of upstream
``hgraph.arrow._pair_operators`` (structural pair detection instead of type
metadata; see ``_arrow`` module docstring)."""
import hgraph as hg
from hgraph import TSB, ts_schema

from ._arrow import _flatten, arrow, make_pair

__all__ = ("first", "swap", "second", "assoc", "flatten_tsl", "to_pair",
           "flatten_tsb")


@arrow(__name__="first")
def first(pair):
    """Returns the first element of a tuple."""
    return pair[0]


@arrow(__name__="swap")
def swap(pair):
    """Swaps the values in a tuple."""
    return make_pair(pair[1], pair[0])


@arrow(__name__="second")
def second(pair):
    """Returns the second element of a tuple."""
    return pair[1]


@arrow(__name__="assoc")
def assoc(pair):
    """((a, b), c) -> (a, (b, c))."""
    return make_pair(pair[0][0], make_pair(pair[0][1], pair[1]))


@arrow(__name__="flatten_tsl")
def flatten_tsl(x):
    """Flattens a (nested) pair into a TSL; all elements must share a type."""
    from hgraph import TSL

    v = _flatten(x)
    return TSL.from_ts(*v)


def flatten_tsb(__schema__=None, **kwargs):
    """Flatten the pairs and assign them to the schema in field order. Takes
    a schema class, a dict of types, or kwargs of types."""
    schema = __schema__ if __schema__ is not None else kwargs
    if isinstance(schema, dict):
        schema = ts_schema(**schema)

    def _wrapper(x):
        v = _flatten(x)
        names = list(schema.__meta_data_schema__) if hasattr(schema, "__meta_data_schema__") \
            else list(getattr(schema, "_field_names", ()))
        if not names:
            names = [f.name for f in getattr(schema, "__dataclass_fields__", {}).values()]
        if not names:
            names = list(getattr(schema, "__annotations__", {}))
        if len(v) != len(names):
            raise ValueError(f"Expected {len(names)} values, got {len(v)}")
        return hg.combine(__output_type__=TSB[schema], **dict(zip(names, v)))

    return arrow(_wrapper, __name__="flatten_tsb")


def to_pair(first_, second_):
    """Convert a named/indexed collection (TSB/TSD/TSL) to a pair via lookup."""

    def _wrapper(x):
        first_v = x[first_] if isinstance(first_, int) else getattr(x, first_)
        second_v = x[second_] if isinstance(second_, int) else getattr(x, second_)
        return make_pair(first_v, second_v)

    return arrow(_wrapper, __name__=f"to_pair({first_}, {second_})")
