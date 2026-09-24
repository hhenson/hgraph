"""``table_schema`` is a native const-evaluable operator (RFC 0042).

The C++ operator publishes released hgraph's ``TableSchema``; Python's
``TableSchema`` is the face of its native schema, bound by name, so the graph
value, the eager ``.value`` and a user-built value are one type.
"""

import datetime

import hgraph as hg
from hgraph import TS, TSD, graph
from hgraph._table import TableSchema
from hgraph.test import eval_node


def test_the_eager_value_outside_any_graph_is_the_graph_value():
    eager = hg.table_schema(TSD[str, TS[int]]).value
    assert type(eager) is TableSchema

    @graph
    def g() -> TS[TableSchema]:
        return hg.table_schema(TSD[str, TS[int]])

    [wired] = eval_node(g)
    assert type(wired) is TableSchema
    assert wired == eager
    assert eager.keys == ("__date_time__", "__as_of__", "__key_1_removed__", "__key_1__", "value")
    assert eager.partition_keys == ("__key_1__",)


def test_types_hold_each_columns_python_class():
    schema = hg.table_schema(TS[datetime.date]).value
    assert schema.types == (datetime.datetime, datetime.datetime, datetime.date)


def test_arrow_types_name_the_columns_of_the_target_frame():
    assert hg.table_schema(TSD[str, TS[float]]).value.arrow_types == (
        "timestamp[us, tz=UTC]",
        "timestamp[us, tz=UTC]",
        "bool",
        "string",
        "double",
    )


def test_a_graph_reads_every_field():
    @graph
    def keys() -> TS[tuple[str, ...]]:
        return hg.getattr_(hg.table_schema(TS[int]), "keys")

    @graph
    def multi_row() -> TS[bool]:
        return hg.getattr_(hg.table_schema(TS[int]), "is_multi_row")

    assert eval_node(keys) == [("__date_time__", "__as_of__", "value")]
    assert eval_node(multi_row) == [False]


def test_evaluate_const_takes_a_time_series_type_argument():
    # A TS[...] expression reaches a const-evaluable type argument, as it does
    # when wiring (the eager call never converted it before).
    with hg.GlobalState():
        value = hg.evaluate_const("table_schema", (TS[int],))
    assert value == hg.table_schema(TS[int]).value
