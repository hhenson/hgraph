from dataclasses import dataclass
from datetime import datetime

from frozendict import frozendict as fd

from hgraph import (
    graph,
    TS,
    to_table,
    table_schema,
    make_table_schema,
    MIN_ST,
    GlobalState,
    set_as_of,
    MIN_TD,
    from_table,
    TSD,
    REMOVE,
    CompoundScalar,
    TimeSeriesSchema,
    TSB,
)
from hgraph.test import eval_node


def test_table_schema_ts_simple_scalar():
    assert table_schema(TS[int]).value == make_table_schema(TS[int], ("value",), (int,))


def test_to_table_ts_simple_scalar():

    @graph
    def table_test(ts: TS[int]) -> TS[tuple[datetime, datetime, int]]:
        return to_table(ts)

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(table_test, [1, 2, 3]) == [
            (MIN_ST, as_of, 1),
            (MIN_ST + MIN_TD, as_of, 2),
            (MIN_ST + MIN_TD * 2, as_of, 3),
        ]


def test_from_table_ts_simple_scalar():
    @graph
    def table_test(ts: TS[int]) -> TS[int]:
        return from_table[TS[int]](to_table(ts))

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(table_test, [1, 2, 3]) == [1, 2, 3]


@dataclass
class MCS(CompoundScalar):
    e: str
    f: float


@dataclass
class MyCompoundScalar(CompoundScalar):
    a: str
    b: int
    c: bool
    d: MCS


def test_table_schema_ts_compound_scalar():
    assert table_schema(TS[MyCompoundScalar]).value == make_table_schema(
        TS[MyCompoundScalar], ("a", "b", "c", "d.e", "d.f"), (str, int, bool, str, float)
    )


def test_to_table_ts_compound_scalar():

    @graph
    def table_test(ts: TS[MyCompoundScalar]) -> TS[tuple[datetime, datetime, str, int, bool, str, float]]:
        return to_table(ts)

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(
            table_test,
            [
                MyCompoundScalar(a="a", b=1, c=True, d=MCS(e="e", f=1.0)),
                MyCompoundScalar(a="b", b=2, c=False, d=MCS(e="f", f=1.1)),
                MyCompoundScalar(a="c", b=3, c=True, d=MCS(e="g", f=1.2)),
            ],
        ) == [
            (MIN_ST, as_of, "a", 1, True, "e", 1.0),
            (MIN_ST + MIN_TD, as_of, "b", 2, False, "f", 1.1),
            (MIN_ST + MIN_TD * 2, as_of, "c", 3, True, "g", 1.2),
        ]


def test_from_table_ts_compound_scalar():
    @graph
    def table_test(ts: TS[MyCompoundScalar]) -> TS[MyCompoundScalar]:
        return from_table[TS[MyCompoundScalar]](to_table(ts))

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(
            table_test,
            [
                MyCompoundScalar(a="a", b=1, c=True, d=MCS(e="e", f=1.0)),
                MyCompoundScalar(a="b", b=2, c=False, d=MCS(e="f", f=1.1)),
                MyCompoundScalar(a="c", b=3, c=True, d=MCS(e="g", f=1.2)),
            ],
        ) == [
            MyCompoundScalar(a="a", b=1, c=True, d=MCS(e="e", f=1.0)),
            MyCompoundScalar(a="b", b=2, c=False, d=MCS(e="f", f=1.1)),
            MyCompoundScalar(a="c", b=3, c=True, d=MCS(e="g", f=1.2)),
        ]


@dataclass
class MyCompoundSchema(TimeSeriesSchema):
    a: TS[str]
    b: TS[int]
    c: TS[bool]
    d: TS[MCS]
    e: TSB[MCS]


def test_table_schema_tsb():
    assert table_schema(TSB[MyCompoundSchema]).value == make_table_schema(
        TSB[MyCompoundSchema], ("a", "b", "c", "d.e", "d.f", "e.e", "e.f"), (str, int, bool, str, float, str, float)
    )


def test_to_table_tsb():

    @graph
    def table_test(ts: TSB[MyCompoundSchema]) -> TS[tuple[datetime, datetime, str, int, bool, str, float, str, float]]:
        return to_table(ts)

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(
            table_test,
            [
                fd(a="a", b=1, c=True, d=MCS(e="e", f=1.0), e=fd(e="f", f=1.1)),
                fd(a="b", b=2, c=False, d=MCS(e="f", f=1.1), e=fd(e="g", f=1.2)),
                fd(a="c", b=3, c=True, d=MCS(e="g", f=1.2), e=fd(e="h", f=1.3)),
            ],
        ) == [
            (MIN_ST, as_of, "a", 1, True, "e", 1.0, "f", 1.1),
            (MIN_ST + MIN_TD, as_of, "b", 2, False, "f", 1.1, "g", 1.2),
            (MIN_ST + MIN_TD * 2, as_of, "c", 3, True, "g", 1.2, "h", 1.3),
        ]


def test_table_schema_tsd_ts_simple_scalar():
    assert table_schema(TSD[str, TS[int]]).value == make_table_schema(
        TSD[str, TS[int]],
        (
            "__key_1_removed__",
            "__key_1__",
            "value",
        ),
        (
            bool,
            str,
            int,
        ),
        ("__key_1__",),
        ("__key_1_removed__",),
    )


def test_to_table_tsd_ts_simple_scalar():

    @graph
    def table_test(ts: TSD[str, TS[int]]) -> TS[tuple[tuple[datetime, datetime, bool, str, int], ...]]:
        return to_table(ts)

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(table_test, [fd({"a": 1}), fd({"a": 2}), fd({"a": REMOVE, "b": 3})]) == [
            ((MIN_ST, as_of, False, "a", 1),),
            ((MIN_ST + MIN_TD, as_of, False, "a", 2),),
            ((MIN_ST + MIN_TD * 2, as_of, False, "b", 3), (MIN_ST + MIN_TD * 2, as_of, True, "a", None)),
        ]


def test_table_schema_tsd_tsd_ts_simple_scalar():
    schema = table_schema(TSD[str, TSD[str, TS[int]]]).value
    assert schema == make_table_schema(
        TSD[str, TSD[str, TS[int]]],
        (
            "__key_1_removed__",
            "__key_1__",
            "__key_2_removed__",
            "__key_2__",
            "value",
        ),
        (
            bool,
            str,
            bool,
            str,
            int,
        ),
        ("__key_1__", "__key_2__"),
        ("__key_1_removed__", "__key_2_removed__"),
    )


def test_to_table_tsd_tsd_ts_simple_scalar():

    @graph
    def table_test(
        ts: TSD[str, TSD[str, TS[int]]],
    ) -> TS[tuple[tuple[datetime, datetime, bool, str, bool, str, int], ...]]:
        return to_table(ts)

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(
            table_test, [fd({"a": fd({"a1": 1})}), fd({"a": fd({"a2": 2})}), fd({"a": REMOVE, "b": fd({"c1": 3})})]
        ) == [
            ((MIN_ST, as_of, False, "a", False, "a1", 1),),
            ((MIN_ST + MIN_TD, as_of, False, "a", False, "a2", 2),),
            (
                (MIN_ST + MIN_TD * 2, as_of, False, "b", False, "c1", 3),
                (MIN_ST + MIN_TD * 2, as_of, True, "a", None, None, None),
            ),
        ]


def test_from_table_tsd_ts_simple_scalar():
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


def test_from_table_tsd_tsd_simple_scalar():
    @graph
    def table_test(ts: TSD[str, TSD[str, TS[int]]]) -> TSD[str, TSD[str, TS[int]]]:
        return from_table[TSD[str, TSD[str, TS[int]]]](to_table(ts))

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(
            table_test, [fd({"a": fd({"a1": 1})}), fd({"a": fd({"b": 2})}), fd({"a": REMOVE, "b": fd({"a": 3})})]
        ) == [fd({"a": fd({"a1": 1})}), fd({"a": fd({"b": 2})}), fd({"a": REMOVE, "b": fd({"a": 3})})]
