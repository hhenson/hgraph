import pytest
from frozendict import frozendict

from hgraph import TS, graph, TIME_SERIES_TYPE, TSD, REMOVE, not_, SCALAR, K, TimeSeriesSchema, TSB, \
    compute_node, REF, TSS, Size, SIZE, K_1, is_empty, Removed
from hgraph.nodes import (make_tsd, extract_tsd, flatten_tsd, sum_, tsd_get_item, const, tsd_rekey, tsd_flip,
                          merge_tsds, tsd_partition, tsd_flip_tsd, tsd_collapse_keys, tsd_uncollapse_keys,
                          merge_nested_tsds, tsd_get_items)
from hgraph.test import eval_node


def test_make_tsd():
    assert eval_node(make_tsd, ['a', 'b', 'a'], [1, 2, 3]) == [{'a': 1}, {'b': 2}, {'a': 3}]


def d(d):
    return frozendict(d)


def test_flatten_expand_tsd():
    @graph
    def flatten_expand_test(ts: TS[frozendict[str, int]]) -> TS[frozendict[str, int]]:
        tsd = extract_tsd[TIME_SERIES_TYPE: TS[int]](ts)
        return flatten_tsd[SCALAR: int](tsd)

    assert eval_node(flatten_expand_test, [{'a': 1}, {'b': 2}, {'a': 3}]) == [{'a': 1}, {'b': 2}, {'a': 3}]


def test_is_empty():
    @graph
    def is_empty_test(tsd: TSD[int, TS[int]]) -> TS[bool]:
        return is_empty(tsd)

    assert eval_node(is_empty_test, [None, {1: 1}, {2: 2}, {1: REMOVE}, {2: REMOVE}]) == [True, False, None, None, True]


def test_not():
    @graph
    def is_empty_test(tsd: TSD[int, TS[int]]) -> TS[bool]:
        return not_(tsd)

    assert eval_node(is_empty_test, [None, {1: 1}, {2: 2}, {1: REMOVE}, {2: REMOVE}]) == [True, False, None, None, True]


@pytest.mark.parametrize(
    ["inputs", "expected"],
    [
        [[{0: 1, 1: 2}, {0: 2, 1: 3}], [3, 5]],
        [[{0: 1.0, 1: 2.0}, {0: 2.0, 1: 3.0}], [3.0, 5.0]],
    ]
)
def test_sum(inputs, expected):
    assert eval_node(sum_, inputs, resolution_dict={'ts': TSD[int, TS[type(inputs[0][0])]]}) == expected


def test_tsd_get_item():
    assert (eval_node(tsd_get_item[K: int, TIME_SERIES_TYPE: TS[int]],
                      [{1: 2, 2: -2}, {1: 3}, {1: 4}, {1: REMOVE}], [None, 1, None, None, 2])
            == [None, 3, 4, None, -2])


def test_tsd_get_items():
    assert (eval_node(tsd_get_items[K: int, TIME_SERIES_TYPE: TS[int]],
                      [{1: 1, 2: 2}, {1: 3}, {1: 4}, {1: REMOVE, 2: 5}, {3: 6}], [None, {1}, {2}, {Removed(2)}, None])
            == [None, {1: 3}, {2: 2, 1: 4}, {2: REMOVE, 1: REMOVE}, None])


def test_tsd_get_bundle_item():
    class TestBundle(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(ts: TSD[int, TSB[TestBundle]]) -> TSD[int, TS[int]]:
        return ts.a

    assert eval_node(g, [{1: dict(a=1, b=2), 2: dict(a=3, b=4)}]) == [{1: 1, 2: 3}]


def test_ref_tsd_key_set():
    @compute_node
    def to_ref(tsd: REF[TSD[str, TS[int]]]) -> REF[TSD[str, TS[int]]]:
        return tsd.value

    @graph
    def main() -> TSS[str]:
        c = const(frozendict(a=1, b=2), TSD[str, TS[int]])
        r = to_ref(c)
        return r.key_set

    assert eval_node(main) == [frozenset(['a', 'b'])]


def test_tsd_rekey():
    fd = frozendict
    assert eval_node(
        tsd_rekey,
        [None, {1: 1}, {2: 2}, None, {2: REMOVE}],
        [{1: "a", 2: "b"}, None, None, {1: "c"}],
        resolution_dict={"ts": TSD[int, TS[int]], "new_keys": TSD[int, TS[str]]}
    ) == [
               None,
               fd({"a": 1}),
               fd({"b": 2}),
               fd({"c": 1, "a": REMOVE}),
               fd({"b": REMOVE})
           ]


def test_tsd_flip():
    fd = frozendict
    assert eval_node(
        tsd_flip,
        [{1: "a", 2: "b"}, {1: "c"}, {2: REMOVE}],
        resolution_dict={"ts": TSD[int, TS[str]]}
    ) == [
               fd({"a": 1, "b": 2}),
               fd({"c": 1, "a": REMOVE}),
               fd({"b": REMOVE})
           ]


def test_tsd_flip_tsd():
    fd = frozendict
    assert eval_node(
        tsd_flip_tsd,
        [
            {1: {"a": 5}, 2: {"b": 6}},
            {1: {"c": 5, "a": REMOVE}},
            {2: REMOVE}
        ],
        resolution_dict={"ts": TSD[int, TSD[str, TS[int]]]}
    ) == [
               fd({"a": fd({1: 5}), "b": fd({2: 6})}),
               fd({"c": fd({1: 5}), "a": REMOVE}),
               fd({"b": REMOVE})
           ]


def test_tsd_collapse_keys():
    fd = frozendict
    assert eval_node(
        tsd_collapse_keys,
        [
            {1: {"a": 5}, 2: {"b": 6}},
            {1: {"c": 5, "a": REMOVE}},
            {2: REMOVE}
        ],
        resolution_dict={"ts": TSD[int, TSD[str, TS[int]]]}
    ) == [
               fd({(1, "a"): 5, (2, "b"): 6}),
               fd({(1, "c"): 5, (1, "a"): REMOVE}),
               fd({(2, "b"): REMOVE})
           ]


def test_tsd_uncollapse_keys():
    fd = frozendict
    assert eval_node(
        tsd_uncollapse_keys,
        [
            {(1, "a"): 5, (2, "b"): 6},
            {(1, "c"): 5, (1, "a"): REMOVE},
            {(2, "b"): REMOVE}
        ],
        resolution_dict={"ts": TSD[tuple[int, str], TS[int]]}
    ) == [
               fd({1: fd({"a": 5}), 2: fd({"b": 6})}, ),
               fd({1: fd({"c": 5, "a": REMOVE})}),
               fd({2: REMOVE})
           ]


def test_merge_tsd():
    assert eval_node(merge_tsds[K: int, TIME_SERIES_TYPE: TS[int], SIZE: Size[2]],
                     [({1: 1, 2: 2}, {1: 5, 3: 6}), ({2: 4}, {3: 8}), ({1: REMOVE}, {}), ({}, {1: REMOVE})]) == [
               {1: 1, 2: 2, 3: 6}, {2: 4, 3: 8}, {1: 5}, {1: REMOVE}]


def test_merge_nested_tsd():
    assert eval_node(merge_nested_tsds[K: int, K_1: int, TIME_SERIES_TYPE: TS[int], SIZE: Size[2]],
                     [({1: {1: 1}, 2: {2: 2}}, {1: {1: 5}, 3: {3: 6}}), ({2: {2: 4}}, {3: {3: 8}}), ({1: REMOVE, 2: {2: REMOVE}}, {}), ({}, {1: REMOVE})]) == [
        {1: {1: 1}, 2: {2: 2}, 3: {3: 6}}, {2: {2: 4}, 3: {3: 8}}, {1: {1: 5}, 2: {2: REMOVE}}, {1: REMOVE}]


def test_tsd_partition():
    assert eval_node(tsd_partition[K: int, K_1: str, TIME_SERIES_TYPE: TS[int]],
                     [{1: 1, 2: 2, 3: 3}, {1: 4, 2: 5, 3: 6}, {1: REMOVE}],
                     [{1: 'odd'}, {2: 'even', 3: 'odd'}, None, {2: REMOVE}, {3: 'prime'}]) == [
               {'odd': {1: 1}},
               {'odd': {1: 4, 3: 6}, 'even': {2: 5}},
               {'odd': {1: REMOVE}},
               {'even': {2: REMOVE}},
               {'prime': {3: 6}, 'odd': {3: REMOVE}}
           ]
