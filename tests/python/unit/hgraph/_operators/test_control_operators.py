import pytest

from hgraph import graph, TS, all_, any_, TSB, TimeSeriesSchema, Size, TSL, SIZE, merge, REF, const, BoolResult, if_, \
    route_by_index, race, if_true, if_then_else
from hgraph.test import eval_node


def test_all_false():
    @graph
    def app() -> TS[bool]:
        return all_(const(True), const(False), const(True))

    assert eval_node(app) == [False]


def test_all_true():
    @graph
    def app() -> TS[bool]:
        return all_(const(True), const(True), const(True))

    assert eval_node(app) == [True]


def test_all_invalid():
    @graph
    def app() -> TS[bool]:
        invalid = if_(True, True).false
        return all_(const(True), const(True), const(True), invalid)

    assert eval_node(app) == [False]


def test_any_false():
    @graph
    def app() -> TS[bool]:
        return any_(const(False), const(False), const(False))

    assert eval_node(app) == [False]


def test_any_true():
    @graph
    def app() -> TS[bool]:
        return any_(const(True), const(False), const(True))

    assert eval_node(app) == [True]


def test_any_invalid():
    @graph
    def app() -> TS[bool]:
        invalid = if_(True, True).false
        return any_(const(True), invalid, const(True), const(True))

    assert eval_node(app) == [True]


def test_if_then_else():
    expected = [
        None,
        2,
        6,
        3
    ]

    assert eval_node(if_then_else, [None, True, False, True], [1, 2, 3], [4, 5, 6]) == expected


@pytest.mark.parametrize("condition,tick_once_only,expected", [
    ([True, False, True], False, [True, None, True]),
    ([True, False, True], True, [True, None, None]),
])
def test_if_true(condition, tick_once_only, expected):
    assert eval_node(if_true, condition, tick_once_only) == expected


def test_if_():
    @graph
    def g(condition: TS[bool], ts: TS[str]) -> TSB[BoolResult[TS[str]]]:
        return if_(condition, ts)

    from frozendict import frozendict as fd
    assert eval_node(g, [True, False, True], ['a', 'b', 'c']) == [
        fd({'true': 'a'}),
        fd({'false': 'b'}),
        fd({'true': 'c'}),
    ]


def test_route_by_index():
    @graph
    def g(index: TS[int], ts: TS[str]) -> TSL[TS[str], Size[4]]:
        return route_by_index[SIZE: Size[4]](index, ts)

    assert eval_node(g, [1, 2, 0, 4], ["1", "2", "2", "2"]) == [{1: "1"}, {2: "2"}, {0: "2"}, None]


def test_merge():
    assert eval_node(merge, [None, 2, None, None, 6], [1, None, 4, None, None], [None, 3, 5, None, None],
                     resolution_dict={"tsl": TSL[TS[int], Size[3]]}) == [1, 2, 4, None, 6]


def test_race_scalars():
    @graph
    def app(tsl: TSL[TS[int], Size[3]]) -> REF[TS[int]]:
        return race(*tsl)

    assert eval_node(app, [{1: 100}, {0: 200, 2: 300}, {1: 300, 2: 500}, {0: 600}]) == [100, None, 300, None]


def test_race_tsls():
    @graph
    def app(invalidate: TS[bool]) -> TS[int]:
        v11 = if_(invalidate, const(11)).false
        v12 = if_(invalidate, const(12)).false
        tsl1 = TSL.from_ts(v11, v12)
        v21 = const(21)
        v22 = const(22)
        tsl2 = TSL.from_ts(v21, v22)
        return race(tsl1, tsl2)[0]

    assert eval_node(app, [False, True]) == [11, 21]


def test_race_tsbs():
    class S(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def app(invalidate: TS[bool]) -> TSB[S]:
        v11 = if_(invalidate, const(11)).false
        v12 = if_(invalidate, const(12)).false
        tsb1 = TSB[S].from_ts(a=v11, b=v12)
        tsb2 = TSB[S].from_ts(a=21, b=22)
        return race(tsb1, tsb2)

    assert eval_node(app, [False, True]) == [{'a': 11, 'b': 12}, {'a': 21, 'b': 22}]
