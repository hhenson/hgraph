from typing import TypeVar

import polars as pl

from hgraph._types import (
    TS,
    Frame,
    COMPOUND_SCALAR,
    CompoundScalar,
    TSD,
    COMPOUND_SCALAR_1,
    COMPOUND_SCALAR_2,
    compound_scalar,
    KEYABLE_SCALAR, TS_SCHEMA, TSB,
)
from hgraph._wiring import compute_node

__all__ = ("join", "filter_frame", "filter_cs", "filter_exp", "filter_exp_seq", "group_by", "ungroup", "sorted_")

ON_TYPE = TypeVar("ON_TYPE", str, tuple[str, ...], pl.Expr)


def _compute_fields(mapping, scalars):
    lhs: CompoundScalar = mapping[COMPOUND_SCALAR].py_type
    rhs = mapping[COMPOUND_SCALAR_1].py_type
    suffix = scalars["suffix"]
    on = scalars["on"]
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
    suffix: str = "_right",
) -> TS[Frame[COMPOUND_SCALAR_2]]:
    """
    Join two data frames together.
    """
    lhs: pl.DataFrame = lhs.value
    return lhs.join(rhs.value, on=on, how=how, suffix=suffix)


# TODO: find a new name for 'filter' to disambiguate with the filter_ operator/
@compute_node
def filter_frame(ts: TS[Frame[COMPOUND_SCALAR]], **predicate: TSB[TS_SCHEMA]) -> TS[Frame[COMPOUND_SCALAR]]:
    kwargs = {k: v for k, v in predicate.value.items() if v is not None}
    return ts.value.filter(**kwargs)


@compute_node
def filter_cs(ts: TS[Frame[COMPOUND_SCALAR]], predicate: TS[COMPOUND_SCALAR]) -> TS[Frame[COMPOUND_SCALAR]]:
    kwargs = {k: v for k, v in predicate.value.to_dict().items() if v is not None}
    return ts.value.filter(**kwargs)


@compute_node
def filter_exp(ts: TS[Frame[COMPOUND_SCALAR]], predicate: pl.Expr) -> TS[Frame[COMPOUND_SCALAR]]:
    return ts.value.filter(predicate)


@compute_node
def filter_exp_seq(ts: TS[Frame[COMPOUND_SCALAR]], predicate: tuple[pl.Expr, ...]) -> TS[Frame[COMPOUND_SCALAR]]:
    return ts.value.filter(predicate)


@compute_node(resolvers={KEYABLE_SCALAR: lambda m, s: m[COMPOUND_SCALAR].py_type.__meta_data_schema__[s["by"]]})
def group_by(ts: TS[Frame[COMPOUND_SCALAR]], by: str) -> TSD[KEYABLE_SCALAR, TS[Frame[COMPOUND_SCALAR]]]:
    return {k: v for (k,), v in ts.value.group_by(by)}


@compute_node
def ungroup(ts: TSD[KEYABLE_SCALAR, TS[Frame[COMPOUND_SCALAR]]]) -> TS[Frame[COMPOUND_SCALAR]]:
    v = [v.value for v in ts.valid_values() if v is not None and len(v.value) > 0]
    if v:
        return pl.concat(v)


@compute_node
def sorted_(ts: TS[Frame[COMPOUND_SCALAR]], by: str, descending: bool = False) -> TS[Frame[COMPOUND_SCALAR]]:
    if not ts.value.is_empty():
        return ts.value.sort(by, descending=descending)
