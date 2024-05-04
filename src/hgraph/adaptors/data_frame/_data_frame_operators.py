from typing import TypeVar, Sequence, Mapping

import polars as pl

from hgraph import TS, Frame, COMPOUND_SCALAR, compute_node, CompoundScalar, graph, WiringError
from hgraph._types._scalar_types import COMPOUND_SCALAR_1, COMPOUND_SCALAR_2, compound_scalar

__all__ = ("join", "filter_cs", "filter_exp", "filter_exp_seq")

ON_TYPE = TypeVar("ON_TYPE", str, tuple[str, ...], pl.Expr)


def _compute_fields(mapping, scalars):
    lhs: CompoundScalar = mapping[COMPOUND_SCALAR].py_type
    rhs = mapping[COMPOUND_SCALAR_1].py_type
    suffix = scalars['suffix']
    on = scalars['on']
    if type(on) is str:
        on = [on]

    lhs_schema = lhs.__meta_data_schema__
    rhs_schema = rhs.__meta_data_schema__

    schema = {k: v.py_type for k, v in lhs_schema.items()}
    for k, v in rhs_schema.items():
        if k in lhs_schema and k not in on:
            schema[f"{k}{suffix}"] = v.py_type
        else:
            schema[k] = v.py_type

    return compound_scalar(**schema)


@compute_node(resolvers={COMPOUND_SCALAR_2: _compute_fields})
def join(
        lhs: TS[Frame[COMPOUND_SCALAR]],
        rhs: TS[Frame[COMPOUND_SCALAR_1]],
        on: ON_TYPE,
        how: str = "inner",
        suffix: str = "_right"
) -> TS[Frame[COMPOUND_SCALAR_2]]:
    """
    Join two data frames together.
    """
    lhs: pl.DataFrame = lhs.value
    return lhs.join(rhs.value, on=on, how=how, suffix=suffix)


@compute_node
def filter_cs(ts: TS[Frame[COMPOUND_SCALAR]], predicate: COMPOUND_SCALAR) -> TS[Frame[COMPOUND_SCALAR]]:
    kwargs = predicate.to_dict()
    return ts.value.filter(**kwargs)


@compute_node
def filter_exp(ts: TS[Frame[COMPOUND_SCALAR]], predicate: pl.Expr) -> TS[Frame[COMPOUND_SCALAR]]:
    return ts.value.filter(predicate)


@compute_node
def filter_exp_seq(ts: TS[Frame[COMPOUND_SCALAR]], predicate: tuple[pl.Expr, ...]) -> TS[Frame[COMPOUND_SCALAR]]:
    return ts.value.filter(predicate)
