from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

import polars as pl
from frozendict import frozendict as fd
from polars.testing import assert_frame_equal

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
    Frame,
    TABLE,
    TableSchema,
)
from hgraph._operators._to_table import ToTableMode
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


def test_table_schema_tsd_tuple_key():
    schema = table_schema(TSD[tuple[int, str], TS[int]]).value
    assert schema == make_table_schema(
        TSD[tuple[int, str], TS[int]],
        (
            "__key_1_removed__",
            "__key_1_0__",
            "__key_1_1__",
            "value",
        ),
        (
            bool,
            int,
            str,
            int,
        ),
        ("__key_1_0__", "__key_1_1__"),
        ("__key_1_removed__",),
    )


def test_table_schema_tsd_complex_key():
    assert table_schema(TSD[MCS, TS[int]]).value == make_table_schema(
        TSD[MCS, TS[int]],
        (
            "__key_1_removed__",
            "__key_1_e__",
            "__key_1_f__",
            "value",
        ),
        (
            bool,
            str,
            float,
            int,
        ),
        ("__key_1_e__", "__key_1_f__"),
        ("__key_1_removed__",),
    )


def test_table_schema_tsd_more_complex_key():
    assert table_schema(TSD[MyCompoundScalar, TS[int]]).value == make_table_schema(
        TSD[MyCompoundScalar, TS[int]],
        (
            "__key_1_removed__",
            "__key_1_a__",
            "__key_1_b__",
            "__key_1_c__",
            "__key_1_d.e__",
            "__key_1_d.f__",
            "value",
        ),
        (
            bool,
            str,
            int,
            bool,
            str,
            float,
            int,
        ),
        ("__key_1_a__", "__key_1_b__", "__key_1_c__", "__key_1_d.e__", "__key_1_d.f__"),
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


def test_to_table_tsd_tsd_ts_bundle():
    @graph
    def table_test(
        ts: TSD[str, TSD[str, TSB[KCS]]],
    ) -> TS[tuple[tuple[datetime, datetime, bool, str, bool, str, str, int], ...]]:
        return to_table(ts)

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(
            table_test, [fd({"a": fd({"a1": {"e": "1"}})}), fd({"a": fd({"a1": {"f": 1}})}), fd({"a": REMOVE})]
        ) == [
            ((MIN_ST, as_of, False, "a", False, "a1", "1", None),),
            ((MIN_ST + MIN_TD, as_of, False, "a", False, "a1", None, 1),),
            ((MIN_ST + MIN_TD * 2, as_of, True, "a", None, None, None, None),),
        ]


def test_to_table_tsd_tsd_ts_bundle_sample():
    @graph
    def table_test(
        ts: TSD[str, TSD[str, TSB[KCS]]],
    ) -> TS[tuple[tuple[datetime, datetime, bool, str, bool, str, str, int], ...]]:
        return to_table(ts, ToTableMode.Sample)

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(
            table_test, [
                fd({"a": fd({"a1": {"e": "1"}, "a2": {"f": 1}})}), 
                fd({"a": fd({"a1": {"f": 1}})}), 
                fd({"a": fd({"a2": REMOVE})}),
                fd({"a": REMOVE}),
                ]
        ) == [
            (
                (MIN_ST, as_of, False, "a", False, "a1", "1", None),
                (MIN_ST, as_of, False, "a", False, "a2", None, 1),
            ),
            ((MIN_ST + MIN_TD, as_of, False, "a", False, "a1", "1", 1),),
            ((MIN_ST + MIN_TD * 2, as_of, False, "a", True, "a2", None, None),),
            ((MIN_ST + MIN_TD * 3, as_of, True, "a", None, None, None, None),),
        ]


@dataclass(frozen=True)
class KCS(CompoundScalar):
    e: str
    f: int


def test_to_table_tsd_tsd_ts_complex_key():

    @graph
    def table_test(
        ts: TSD[str, TSD[KCS, TS[int]]],
    ) -> TS[tuple[tuple[datetime, datetime, bool, str, bool, str, int, int], ...]]:
        return to_table(ts)

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        assert eval_node(
            table_test, [fd({"a": fd({KCS(e="a1", f=1): 1})}), fd({"a": fd({KCS(e="a2", f=1): 2})}), fd({"a": REMOVE, "b": fd({KCS(e="c1", f=1): 3})})]
        ) == [
            ((MIN_ST, as_of, False, "a", False, "a1", 1, 1),),
            ((MIN_ST + MIN_TD, as_of, False, "a", False, "a2", 1, 2),),
            (
                (MIN_ST + MIN_TD * 2, as_of, False, "b", False, "c1", 1, 3),
                (MIN_ST + MIN_TD * 2, as_of, True, "a", None, None, None, None),
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


def test_to_table_from_table_frame():
    @dataclass(frozen=True)
    class MySchema(CompoundScalar):
        a: int
        b: int

    @graph
    def g(ts: TS[Frame[MySchema]]) -> TS[Frame[MySchema]]:
        return from_table[TS[Frame[MySchema]]](to_table(ts))

    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    assert_frame_equal(eval_node(g, [df])[-1], df)


def test_to_table_frame():
    @dataclass(frozen=True)
    class MySchema(CompoundScalar):
        a: int
        b: int

    @graph
    def g(ts: TS[Frame[MySchema]]) -> TS[TABLE]:
        return to_table(ts)

    with GlobalState() as gs:
        as_of = MIN_ST + 10 * MIN_TD
        set_as_of(as_of)
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        assert eval_node(g, [df])[-1] == (MIN_ST, as_of, (1, 4), (2, 5), (3, 6))


def test_to_table_schema_frame():
    @dataclass(frozen=True)
    class MySchema(CompoundScalar):
        a: int
        b: int

    assert table_schema(TS[Frame[MySchema]]).value == TableSchema(
        tp=TS[Frame[MySchema]],
        keys=("__date_time__", "__as_of__", "a", "b"),
        types=(datetime, datetime, int, int),
        partition_keys=(),
        removed_keys=(),
        date_time_key="__date_time__",
        as_of_key="__as_of__",
    )
