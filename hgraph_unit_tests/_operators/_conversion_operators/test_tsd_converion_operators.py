from typing import Mapping, Set, Tuple
from frozendict import frozendict as fd

from hgraph import TS, combine, TSD, graph, convert, REMOVE, Size, TSL, collect, TSS, Removed, TSB, emit, KeyValue
from hgraph.test import eval_node


def test_convert_ts_to_tsd():
    @graph
    def g(a: TS[str], b: TS[int]) -> TSD[str, TS[int]]:
        return convert[TSD](a, b)

    assert eval_node(g, ["a", "b"], [1, 2]) == [{"a": 1}, {"b": 2, "a": REMOVE}]

    @graph
    def g(a: TS[str], b: TS[int]) -> TSD[str, TS[int]]:
        return convert[TSD[str, TS[int]]](a, b)

    assert eval_node(g, ["a", "b"], [1, 2]) == [{"a": 1}, {"b": 2, "a": REMOVE}]


def test_convert_set_to_tsd():
    @graph
    def g(a: TS[Set[str]], b: TS[int]) -> TSD[str, TS[int]]:
        return convert[TSD](a, b)

    assert eval_node(g, [{"a"}, {"b"}], [1, 2, 3]) == [{"a": 1}, {"b": 2, "a": REMOVE}, {"b": 3}]


def test_convert_tss_to_tsd():
    @graph
    def g(a: TSS[str], b: TS[int]) -> TSD[str, TS[int]]:
        return convert[TSD](a, b)

    assert eval_node(g, [{"a"}, {"b", Removed("a")}], [1, 2, 3]) == [{"a": 1}, {"b": 2, "a": REMOVE}, {"b": 3}]


def test_convert_tsl_to_tsd():
    @graph
    def g(a: TSL[TS[int], Size[2]]) -> TSD[int, TS[int]]:
        return convert[TSD](a)

    assert eval_node(g, [(None, 1), (2, None)]) == [{1: 1}, {0: 2}]


def test_convert_mapping_to_tsd():
    @graph
    def g(a: TS[Mapping[int, int]]) -> TSD[int, TS[int]]:
        return convert[TSD](a)

    assert eval_node(g, [{1: 2}, {3: 4}]) == [{1: 2}, {3: 4, 1: REMOVE}]


def test_combine_tsd_from_tuple_and_tsl():
    @graph
    def g(a: TS[int], b: TS[int]) -> TSD[str, TS[int]]:
        return combine[TSD](("a", "b"), a, b)

    assert eval_node(g, [1, 2], [3, None]) == [{"a": 1, "b": 3}, {"a": 2}]

    @graph
    def g(a: TS[int], b: TS[int]) -> TSD[str, TS[int]]:
        return combine[TSD[str, TS[int]]](("a", "b"), a, b)

    assert eval_node(g, [1, 2], [3, None]) == [{"a": 1, "b": 3}, {"a": 2}]


def test_combine_tsd_from_tuple_and_tuple():
    @graph
    def g(k: TS[tuple[str, ...]], v: TS[tuple[int, ...]]) -> TSD[str, TS[int]]:
        return combine[TSD](k, v)

    result= eval_node(g, [("a", "b"), None, ("a", "c")], [(1, 2), (1, 3), None])
    assert result == [fd({"a": 1, "b": 2}), fd({"b": 3}), fd({"b": REMOVE, "c": 3})]


def test_collect_tsd():
    @graph
    def g(k: TS[str], v: TS[int], b: TS[bool]) -> TSD[str, TS[int]]:
        return collect[TSD](k, v, reset=b)

    assert eval_node(g, ["a", "b", "c"], [1, 2, 3, 4], [None, None, True]) == [
        {"a": 1},
        {"b": 2},
        {"c": 3, "a": REMOVE, "b": REMOVE},
        {"c": 4},
    ]


def test_collect_tsd_from_tuples():
    @graph
    def g(k: TS[Tuple[str, ...]], v: TS[Tuple[int, ...]], b: TS[bool]) -> TSD[str, TS[int]]:
        return collect[TSD](k, v, reset=b)

    assert eval_node(g, [("a",), ("b", "c"), ("c",)], [(1,), (2, 3), (3,), (4,)], [None, None, True]) == [
        {"a": 1},
        {"b": 2, "c": 3},
        {"c": 3, "a": REMOVE, "b": REMOVE},
        {"c": 4},
    ]


def test_collect_tsd_from_mappings():
    @graph
    def g(a: TS[Mapping[str, int]], b: TS[bool]) -> TSD[str, TS[int]]:
        return collect[TSD](a, reset=b)

    assert eval_node(g, [{"a": 1}, {"b": 2, "c": 3}, {"c": 3}, {"c": 4}], [None, None, True]) == [
        {"a": 1},
        {"b": 2, "c": 3},
        {"c": 3, "a": REMOVE, "b": REMOVE},
        {"c": 4},
    ]


def test_collect_tsd_from_tsd():
    @graph
    def g(a: TSD[str, TS[int]], b: TS[bool]) -> TSD[str, TS[int]]:
        return collect[TSD](a, reset=b)

    assert eval_node(g, [{"a": 1}, {"b": 2, "c": 3}, {"c": 3}, {"c": 4}], [None, None, True]) == [
        {"a": 1},
        {"b": 2, "c": 3},
        {"c": 3, "a": REMOVE, "b": REMOVE},
        {"c": 4},
    ]


def test_collect_tsd_from_tsd_with_excludes():
    @graph
    def g(a: TSD[str, TS[int]], exclude: TSS[str]) -> TSD[str, TS[int]]:
        return collect[TSD](a, exclude=exclude)

    assert eval_node(g, [{"a": 1}, {"b": 2, "c": 3}, {"c": 3}, {"c": 4}], [{"c"}, None, None, {"b"}]) == [
        {"a": 1},
        {"b": 2},
        None,
        {"b": REMOVE},
    ]


def test_emit_tsd():
    @graph
    def g(m: TSD[str, TS[int]]) -> TSB[KeyValue[str, TS[int]]]:
        return emit(m)

    actual = eval_node(g, [{"a": 1, "b": 2, "c": 3}, None, {"a": 4}])

    # First tick emits one event per key in m; key order is not guaranteed by C++.
    first_batch = sorted(actual[:3], key=lambda e: e["key"])  # order-agnostic within the tick
    expected_first_batch = [
        {"key": "a", "value": 1},
        {"key": "b", "value": 2},
        {"key": "c", "value": 3},
    ]
    assert first_batch == expected_first_batch
    # Third input tick emits only 'a'; preserve cross-tick sequence
    assert actual[3] == {"key": "a", "value": 4}

    @graph
    def h(m: TSD[str, TS[Tuple[int, int]]]) -> TSB[KeyValue[str, TSL[TS[int], Size[2]]]]:
        return emit[TSL[TS[int], Size[2]]](m)

    actual2 = eval_node(h, [{"a": (1, 1), "b": (2, 2), "c": (3, 3)}, None, {"a": (4, 4)}])

    first_batch2 = sorted(actual2[:3], key=lambda e: e["key"])  # order-agnostic within the tick
    expected_first_batch2 = [
        {"key": "a", "value": {0: 1, 1: 1}},
        {"key": "b", "value": {0: 2, 1: 2}},
        {"key": "c", "value": {0: 3, 1: 3}},
    ]
    assert first_batch2 == expected_first_batch2
    assert actual2[3] == {"key": "a", "value": {0: 4, 1: 4}}


def test_convert_tuple_to_enumerated_tsd():
    @graph
    def g(ts: TS[Tuple[str, ...]]) -> TSD[int, TS[str]]:
        return convert[TSD[int, TS[str]]](ts)

    assert eval_node(g, [None, ("0",), ("0", "1"), ("1",), ("3",)]) == [
        None,
        {0: "0"},
        {1: "1"},
        {0: "1", 1: REMOVE},
        {0: "3"},
    ]
