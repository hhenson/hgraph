# Ported from release/0.5:hgraph_unit_tests/_operators/test_to_table.py
# Changes from upstream:
#  - polars -> pyarrow (the Arrow ruling: Frame values cross the bridge as
#    pyarrow tables; assert_frame_equal -> Table.equals).
#  - `from hgraph._operators._to_table import ToTableMode` -> `from hgraph
#    import ToTableMode` (upstream-internal module path).
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

import pyarrow as pa
import pytest
from frozendict import frozendict as fd

from hgraph import (
    graph,
    TS,
    to_table,
    table_schema,
    table_shape,
    table_shape_from_schema,
    shape_of_table_type,
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
    ToTableMode,
    WiringError,
)
from hgraph.test import eval_node


def test_table_schema_ts_simple_scalar():
    assert table_schema(TS[int]).value == make_table_schema(TS[int], ("value",), (int,))


def test_table_shape_helpers_follow_the_native_layout():
    scalar_schema = table_schema(TS[int]).value
    scalar_shape = table_shape_from_schema(scalar_schema)
    assert scalar_shape == tuple[datetime, datetime, int]
    assert table_shape(TS[int]) == scalar_shape
    assert shape_of_table_type(scalar_shape, expect_keys=False, expect_length=1) == ((int,), False)

    keyed_shape = table_shape(TSD[str, TS[int]])
    assert keyed_shape == tuple[tuple[datetime, datetime, bool, str, int], ...]
    assert shape_of_table_type(keyed_shape, expect_keys=True, expect_length=2) == ((str, int), True)

    with pytest.raises(WiringError, match="expected a keyed table shape"):
        shape_of_table_type(scalar_shape, expect_keys=True)


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

    df = pa.table({"a": [1, 2, 3], "b": [4, 5, 6]})
    assert eval_node(g, [df])[-1].equals(df)


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
        df = pa.table({"a": [1, 2, 3], "b": [4, 5, 6]})
        # Each row gets its own datetime columns
        assert eval_node(g, [df])[-1] == ((MIN_ST, as_of, 1, 4), (MIN_ST, as_of, 2, 5), (MIN_ST, as_of, 3, 6))


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
        is_multi_row=True,  # Frame returns multiple rows (one per DataFrame row)
    )


def test_unpinned_as_of_is_the_wall_clock_and_round_trips():
    """``__date_time__`` is when the value was true; ``__as_of__`` is when we
    came to believe it.

    Defaulting as-of to the evaluation time made the two columns equal, so
    as-of carried no information and the replay path's revision filter had
    nothing to select on (issue #810 item 4.14). Both halves of that contract
    moved together, and this covers both: the recording stamps a real
    timestamp, and an unpinned replay still selects the row rather than
    filtering it away as later than its cutoff.
    """
    from datetime import timedelta, timezone

    from hgraph import MIN_ST, TS, from_table, graph, to_table
    from hgraph.test import eval_node

    @graph
    def emit(ts: TS[int]) -> TS[tuple]:
        return to_table(ts)

    before = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = eval_node(emit, [1, 2])
    after = datetime.now(timezone.utc).replace(tzinfo=None)

    # The recording stamps as-of from the C++ ``engine_clock`` while ``before``
    # and ``after`` come from CPython's clock. Both read the system wall clock,
    # but on Windows they reach it through different APIs and are not mutually
    # ordered to the microsecond, so bracketing exactly is flaky (a run has been
    # seen ~0.5ms outside it). The property under test is that as-of is a real
    # recent timestamp rather than the evaluation time, and MIN_ST is decades
    # away -- a second of slack separates those two answers with room to spare.
    slack = timedelta(seconds=1)

    assert len(rows) == 2
    for index, row in enumerate(rows):
        date_time, as_of, value = row
        # Unchanged: the evaluation time, which simulation starts at MIN_ST.
        assert date_time == MIN_ST + index * (MIN_ST.resolution)
        assert before - slack <= as_of <= after + slack
        assert as_of != date_time
        assert value == index + 1

    # The paired half: a round trip with NOTHING pinned still selects the row.
    # A replay cutoff left at the graph's start_time would sit decades before
    # a wall-clock revision and filter the whole frame away.
    @graph
    def round_trip(ts: TS[int]) -> TS[int]:
        return from_table[TS[int]](to_table(ts))

    assert eval_node(round_trip, [1, 2]) == [1, 2]
