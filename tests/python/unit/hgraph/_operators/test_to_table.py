from datetime import datetime

from frozendict import frozendict as fd

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
    TSD,
    REMOVE,
)
from hgraph.test import eval_node


def test_simple_scalar():

    @graph
    def table_test(ts: TS[int]) -> TS[tuple[datetime, datetime, int]]:
        return to_table(ts)

    @graph
    def table_test_schema() -> TS[TableSchema]:
        return table_schema(TS[int])

    assert eval_node(table_test_schema) == [make_table_schema(TS[int], ("value",), (int,))]

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


def test_tsd_ts_simple_scalar():

    @graph
    def table_test(ts: TSD[str, TS[int]]) -> TS[tuple[tuple[datetime, datetime, bool, str, int], ...]]:
        return to_table(ts)

    @graph
    def table_test_schema() -> TS[TableSchema]:
        return table_schema(TSD[str, TS[int]])

    assert eval_node(table_test_schema) == [
        make_table_schema(
            TSD[str, TS[int]],
            (
                "key",
                "value",
            ),
            (
                str,
                int,
            ),
            ("key",),
        )
    ]

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(table_test, [fd({"a": 1}), fd({"a": 2}), fd({"a": REMOVE, "b": 3})]) == [
            ((MIN_ST, as_of, False, "a", 1),),
            ((MIN_ST + MIN_TD, as_of, False, "a", 2),),
            ((MIN_ST + MIN_TD * 2, as_of, False, "b", 3), (MIN_ST + MIN_TD * 2, as_of, True, "a", None)),
        ]


def test_tsd_ts_simple_from_scalar():
    @graph
    def table_test(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return from_table[TSD[str, TS[int]]](to_table(ts))

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(table_test, [fd({"a": 1}), fd({"a": 2}), fd({"a": REMOVE, "b": 3})]) == [
            fd({"a": 1}),
            fd({"a": 2}),
            fd({"a": REMOVE, "b": 3}),
        ]
