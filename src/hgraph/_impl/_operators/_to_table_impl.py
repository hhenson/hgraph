from datetime import date, datetime, time, timedelta
from itertools import chain
from typing import TypeVar

from hgraph import AUTO_RESOLVE, from_table, TSD, K, REMOVE_IF_EXISTS
from hgraph._operators import to_table, get_as_of, table_schema, TableSchema, make_table_schema
from hgraph._operators._to_table import from_table_const, TABLE, table_shape, shape_of_table_type
from hgraph._runtime import EvaluationClock
from hgraph._types import TS, SCALAR
from hgraph._wiring._decorators import compute_node, const_fn

__all__ = []


SIMPLE_SCALAR = TypeVar("SIMPLE_SCALAR", bool, int, float, str, date, datetime, time, timedelta)


@compute_node(overloads=to_table, resolvers={TABLE: lambda m, s: table_shape(TS[m[SIMPLE_SCALAR]])})
def to_table_ts_simple_value(ts: TS[SIMPLE_SCALAR], _clock: EvaluationClock = None) -> TS[TABLE]:
    return (ts.last_modified_time, get_as_of(_clock), ts.value)


@const_fn(overloads=table_schema)
def table_schema_ts_simple_value(tp: type[TS[SCALAR]], _scalar: type[SCALAR] = AUTO_RESOLVE) -> TS[TableSchema]:
    return make_table_schema(TS[_scalar], ("value",), (_scalar,))


@compute_node(
    overloads=from_table, resolvers={SIMPLE_SCALAR: lambda m, s: shape_of_table_type(m[TABLE], False, 1)[0][0]}
)
def from_table_ts_simple_value(ts: TS[TABLE]) -> TS[SIMPLE_SCALAR]:
    # We can ignore the date information as this would have been used to inject the data
    return ts.value[-1]


@const_fn(
    overloads=from_table_const, resolvers={SIMPLE_SCALAR: lambda m, s: shape_of_table_type(m[TABLE], False, 1)[0][0]}
)
def from_table_const_ts_simple_value(value: TABLE) -> TS[SIMPLE_SCALAR]:
    return value[-1]


# TSD[K, TS[SCALAR]]


@compute_node(overloads=to_table, resolvers={TABLE: lambda m, s: table_shape(TSD[m[K], TS[m[SIMPLE_SCALAR]]])})
def to_table_tsd_ts_simple_value(ts: TSD[K, TS[SIMPLE_SCALAR]], _clock: EvaluationClock = None) -> TS[TABLE]:
    return tuple(
        chain(
            ((ts.last_modified_time, get_as_of(_clock), False, k, v.value) for k, v in ts.modified_items()),
            ((ts.last_modified_time, get_as_of(_clock), True, k, None) for k in ts.removed_keys()),
        )
    )


@const_fn(overloads=table_schema)
def table_schema_tsd_ts_simple_value(
    tp: type[TSD[K, TS[SIMPLE_SCALAR]]], _key: type[K] = AUTO_RESOLVE, _scalar: type[SIMPLE_SCALAR] = AUTO_RESOLVE
) -> TS[TableSchema]:
    return make_table_schema(
        tp,
        (
            "key",
            "value",
        ),
        (
            _key,
            _scalar,
        ),
        partition_keys=("key",),
    )


@compute_node(
    overloads=from_table,
    resolvers={
        K: lambda m, s: shape_of_table_type(m[TABLE], True, 2)[0][0],
        SIMPLE_SCALAR: lambda m, s: shape_of_table_type(m[TABLE], True, 2)[0][1],
    },
)
def from_table_tsd_ts_simple_value(ts: TS[TABLE]) -> TSD[K, TS[SIMPLE_SCALAR]]:
    # We can ignore the date information as this would have been used to inject the data
    v = ts.value
    return _tsd_ts_simple_value(v)


def _tsd_ts_simple_value(value: TABLE) -> dict:
    out = {r[-2]: REMOVE_IF_EXISTS if r[2] else r[-1] for r in value}
    return out


@const_fn(
    overloads=from_table_const,
    resolvers={
        K: lambda m, s: shape_of_table_type(m[TABLE], True, 2)[0][0],
        SIMPLE_SCALAR: lambda m, s: shape_of_table_type(m[TABLE], True, 2)[0][1],
    },
)
def from_table_const_tsd_ts_simple_value(value: TABLE) -> TSD[K, TS[SIMPLE_SCALAR]]:
    return _tsd_ts_simple_value(value)
