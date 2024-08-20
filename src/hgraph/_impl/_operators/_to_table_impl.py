from datetime import date, datetime, time, timedelta
from typing import TypeVar

from hgraph import AUTO_RESOLVE
from hgraph._runtime import EvaluationClock
from hgraph._operators import to_table, get_as_of, to_table_schema, const, TableSchema, make_table_schema
from hgraph._types import TS, TSB, SCALAR
from hgraph._wiring._decorators import compute_node, graph

__all__ = []


SIMPLE_SCALAR = TypeVar("SIMPLE_SCALAR", bool, int, float, str, date, datetime, time, timedelta)


def _simple_value_shape(m, s):
    return tuple[datetime, m[SIMPLE_SCALAR].py_type]


@compute_node(overloads=to_table, resolvers={SCALAR: _simple_value_shape})
def to_table_simple_value(ts: TS[SIMPLE_SCALAR], _clock: EvaluationClock = None) -> TS[SCALAR]:
    return (ts.last_modified_time, get_as_of(_clock), ts.value)


@graph(overloads=to_table_schema)
def to_table_schema_value(tp: type[TS[SCALAR]], _scalar: type[SCALAR] = AUTO_RESOLVE) -> TS[TableSchema]:
    return const(make_table_schema(("value",), (_scalar,)))
