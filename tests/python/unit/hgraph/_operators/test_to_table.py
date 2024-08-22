from datetime import datetime

from hgraph import (
    graph,
    TS,
    to_table,
    TableSchema,
    table_schema,
    make_table_schema,
    MIN_ST,
    GlobalState,
    set_as_of,
    MIN_TD,
    from_table,
)
from hgraph.test import eval_node


def test_simple_scalar():

    @graph
    def table_test(ts: TS[int]) -> TS[tuple[datetime, datetime, int]]:
        return to_table(ts)

    @graph
    def table_test_schema() -> TS[TableSchema]:
        return table_schema(TS[int])

    assert eval_node(table_test_schema) == [make_table_schema(("value",), (int,))]

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(table_test, [1, 2, 3]) == [
            (MIN_ST, as_of, 1),
            (MIN_ST + MIN_TD, as_of, 2),
            (MIN_ST + MIN_TD * 2, as_of, 3),
        ]


def test_simple_from_scalar():
    @graph
    def table_test(ts: TS[int]) -> TS[int]:
        return from_table[TS[int]](to_table(ts))

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(table_test, [1, 2, 3]) == [1, 2, 3]
