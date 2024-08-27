from datetime import date, datetime, time, timedelta
from typing import TypeVar

from hgraph import AUTO_RESOLVE, from_table, HgTupleFixedScalarType
from hgraph._operators import to_table, get_as_of, table_schema, const, TableSchema, make_table_schema
from hgraph._operators._to_table import from_table_const
from hgraph._runtime import EvaluationClock
from hgraph._types import TS, SCALAR
from hgraph._wiring._decorators import compute_node, graph, const_fn

__all__ = []


SIMPLE_SCALAR = TypeVar("SIMPLE_SCALAR", bool, int, float, str, date, datetime, time, timedelta)


def _simple_value_shape(m, s):
    return tuple[datetime, datetime, m[SIMPLE_SCALAR].py_type]


@compute_node(overloads=to_table, resolvers={SCALAR: _simple_value_shape})
def to_table_ts_simple_value(ts: TS[SIMPLE_SCALAR], _clock: EvaluationClock = None) -> TS[SCALAR]:
    return (ts.last_modified_time, get_as_of(_clock), ts.value)


@const_fn(overloads=table_schema)
def table_schema_ts_simple_value(tp: type[TS[SCALAR]], _scalar: type[SCALAR] = AUTO_RESOLVE) -> TS[TableSchema]:
    return make_table_schema(("value",), (_scalar,))


def _simple_value_scalar(m, s):
    tpl_type = m[SCALAR]
    assert type(tpl_type) is HgTupleFixedScalarType, "Input should be a HgTupleFixedScalarType"
    types = tpl_type.element_types
    assert len(types) == 3  # Expect date, as_of, value
    return types[-1].py_type


@compute_node(overloads=from_table, resolvers={SIMPLE_SCALAR: _simple_value_scalar})
def from_table_ts_simple_value(ts: TS[SCALAR]) -> TS[SIMPLE_SCALAR]:
    # We can ignore the date information as this would have been used to inject the data
    return ts.value[-1]


@const_fn(overloads=from_table_const, resolvers={SIMPLE_SCALAR: _simple_value_scalar})
def from_table_const_ts_simple_value(value: SCALAR) -> TS[SIMPLE_SCALAR]:
    return value[-1]
