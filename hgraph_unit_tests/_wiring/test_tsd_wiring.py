from hgraph import (
    compute_node,
    TS,
    TSD,
    graph,
    TSS,
    REMOVE,
    contains_,
    TimeSeriesSchema,
    TSB,
    map_,
    TSL,
    Size,
    feedback,
    REMOVE_IF_EXISTS,
)
from hgraph.test import eval_node


import pytest
pytestmark = pytest.mark.smoke


@compute_node
def make_tsd(k: TS[str], v: TS[int]) -> TSD[str, TS[int]]:
    return {k.value: v.delta_value}


def test_tsd():
    assert eval_node(make_tsd, k=["a", "b"], v=[1, 2]) == [{"a": 1}, {"b": 2}]


def test_tsd_key_set():
    @graph
    def _extract_key_set(tsd: TSD[str, TS[int]]) -> TSS[str]:
        return tsd.key_set

    assert eval_node(_extract_key_set, tsd=[{"a": 1}, {"b": 2}]) == [{"a"}, {"b"}]


def test_tsd_get_item():
    @graph
    def main(tsd: TSD[str, TS[int]], k: TS[str]) -> TS[int]:
        return tsd[k]

    assert eval_node(main, [{"a": 1}, {"b": 2}, {"b": 3}, {}, {"a": REMOVE}], ["b", None, None, "a"]) == [
        None,
        2,
        3,
        1,
        None,
    ]


def test_tsd_contains():

    @graph
    def main(tsd: TSD[str, TS[int]], k: TS[str]) -> TS[bool]:
        return contains_(tsd, k)

    assert eval_node(main, [{"a": 1}, {"b": 2}, {"b": 3}, {}, {"a": REMOVE}], ["b", None, None, "a"]) == [
        False,
        True,
        None,
        True,
        False,
    ]


def test_fast_non_peer_tsd():
    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(tsd1: TSD[str, TS[int]], tsd2: TSD[str, TS[int]]) -> TSD[str, TSB[AB]]:
        return copy_tsd(map_(lambda x, y: TSB[AB].from_ts(a=x, b=y), tsd1, tsd2))

    @compute_node
    def copy_tsd(tsd: TSD[str, TSB[AB]]) -> TSD[str, TSB[AB]]:
        return dict((k, v.delta_value) for k, v in zip(tsd.modified_keys(), tsd.modified_values()))

    assert eval_node(g, [{"x": 1, "y": 2}, {}], [{}, {"x": 7, "y": 8}]) == [
        {"x": {"a": 1}, "y": {"a": 2}},
        {"x": {"b": 7}, "y": {"b": 8}},
    ]


def test_tsd_add_remove_in_same_cycle():
    @compute_node
    def add_remove(a: TS[bool], _output: TSD = None) -> TSD[str, TS[int]]:
        _output.get_or_create("a").value = 1
        del _output["a"]
        assert "a" not in _output.removed_keys()

    assert eval_node(add_remove, [True]) == [{}]


def test_tsd_add_invalid_and_remove():
    @compute_node
    def add_remove(a: TS[bool], _output: TSD = None) -> TSD[str, TS[int]]:
        _output.get_or_create("a").value = None
        assert "a" in _output
        if a.value:
            del _output["a"]
            assert "a" not in _output

    assert eval_node(add_remove, [False, True]) == [{}, {}]


def test_tsd_add_clear_in_same_cycle():
    @compute_node
    def add_remove(a: TS[bool], _output: TSD = None) -> TSD[str, TS[int]]:
        _output.get_or_create("a").value = 1
        _output.clear()

    @graph
    def main(a: TS[bool]) -> TSD[str, TS[int]]:
        r = feedback(add_remove(a))()
        return r

    assert eval_node(main, [True]) == [None, {}]
