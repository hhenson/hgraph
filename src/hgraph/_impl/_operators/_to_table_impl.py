from datetime import date, datetime, time, timedelta
from typing import TypeVar

from hgraph import AUTO_RESOLVE, from_table, TSD, K
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


def _tsd_ts_simple_value_schema(m, s):
    k = m[K]
    v = m[SCALAR]


@compute_node(overloads=to_table, resolvers={TABLE: _tsd_ts_simple_value_schema})
def to_table_tsd_ts_simple_value(ts: TSD[K, TS[SCALAR]], _clock: EvaluationClock = None) -> TS[TABLE]: ...
