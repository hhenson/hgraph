from typing import Set, Tuple

import pytest
from frozendict import frozendict

from hgraph import TS, graph, TIME_SERIES_TYPE, TSD, REMOVE, not_, SCALAR, K, TimeSeriesSchema, TSB, \
    compute_node, REF, TSS, Size, SIZE, K_1, is_empty, Removed, sub_, eq_, len_, min_, max_, keys_, OUT, rekey, flip, \
    partition, flip_keys, collapse_keys, uncollapse_keys, str_, sum_, const, getitem_, merge, TSL
from hgraph.nodes import make_tsd, extract_tsd, flatten_tsd
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


def test_sub_tsds():
    assert eval_node(sub_, [{0: 1, 3: 2}], [{0: 2, 1: 3}],
                     resolution_dict={'lhs': TSD[int, TS[int]], 'rhs': TSD[int, TS[int]]}) == [frozendict({3: 2})]


def test_tsd_get_item():
    assert (
            eval_node(
                getitem_,
                [{1: 2, 2: -2}, {1: 3}, {1: 4}, {1: REMOVE}],
                [None, 1, None, None, 2],
                resolution_dict={'ts': TSD[int, TS[int]], 'key': TS[int]}
            )
            == [None, 3, 4, None, -2])


def test_tsd_get_items():
    assert (
            eval_node(
                getitem_,
                [{1: 1, 2: 2}, {1: 3}, {1: 4}, {1: REMOVE, 2: 5}, {3: 6}],
                [None, {1}, {2}, {Removed(2)}, None],
                resolution_dict={'ts': TSD[int, TS[int]], 'key': TSS[int]}
            )
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


def test_rekey():
    @graph
    def g(ts: TSD[int, TS[int]], new_keys: TSD[int, TS[str]]) -> TSD[str, TS[int]]:
        return rekey(ts, new_keys)

    fd = frozendict
    assert eval_node(
        g,
        [None, {1: 1, 3: 3}, {2: 2}, None, {2: REMOVE}],
        [{1: "a", 2: "b"}, None, None, {1: "c"}]
    ) == [
               None,
               fd({"a": 1}),
               fd({"b": 2}),
               fd({"c": 1, "a": REMOVE}),
               fd({"b": REMOVE})
           ]


def test_flip():
    @graph
    def g(ts: TSD[int, TS[str]]) -> TSD[str, TS[int]]:
        return flip(ts)

    fd = frozendict
    assert eval_node(
        g,
        [{1: "a", 2: "b"}, {1: "c"}, {2: REMOVE}]
    ) == [
               fd({"a": 1, "b": 2}),
               fd({"c": 1, "a": REMOVE}),
               fd({"b": REMOVE})
           ]


def test_flip_keys():
    @graph
    def g(ts: TSD[int, TSD[str, TS[int]]]) -> TSD[str, TSD[int, TS[int]]]:
        return flip_keys(ts)

    fd = frozendict
    assert eval_node(
        g,
        [
            {1: {"a": 5, "d": 4}, 2: {"b": 6}},
            {1: {"c": 5, "a": REMOVE}},
            {2: REMOVE}
        ]
    ) == [
               fd({"a": fd({1: 5}), "d": fd({1: 4}), "b": fd({2: 6})}),
               fd({"c": fd({1: 5}), "a": REMOVE}),
               fd({"b": REMOVE})
           ]


def test_collapse_keys():
    @graph
    def g(ts: TSD[int, TSD[str, TS[int]]]) -> TSD[Tuple[int, str], TS[int]]:
        return collapse_keys(ts)

    fd = frozendict
    assert eval_node(
        g,
        [
            {1: {"a": 5}, 2: {"b": 6}},
            {1: {"c": 5, "a": REMOVE}},
            {2: REMOVE}
        ]
    ) == [
               fd({(1, "a"): 5, (2, "b"): 6}),
               fd({(1, "c"): 5, (1, "a"): REMOVE}),
               fd({(2, "b"): REMOVE})
           ]


def test_uncollapse_keys():
    @graph
    def g(ts: TSD[Tuple[int, str], TS[int]]) -> TSD[int, TSD[str, TS[int]]]:
        return uncollapse_keys(ts)

    fd = frozendict
    assert eval_node(
        g,
        [
            {(1, "a"): 5, (2, "b"): 6},
            {(1, "c"): 5, (1, "a"): REMOVE},
            {(2, "b"): REMOVE}
        ]
    ) == [
               fd({1: fd({"a": 5}), 2: fd({"b": 6})}, ),
               fd({1: fd({"c": 5, "a": REMOVE})}),
               fd({2: REMOVE})
           ]


def test_merge_tsd():
    assert (
            eval_node(
                merge[K: int, TIME_SERIES_TYPE: TS[int], SIZE: Size[2]],
                tsl=[({1: 1, 2: 2}, {1: 5, 3: 6}), ({2: 4}, {3: 8}), ({1: REMOVE}, {}), ({}, {1: REMOVE})],
                resolution_dict={'tsl': TSL[TSD[int, TS[int]], Size[2]]}
            ) == [{1: 1, 2: 2, 3: 6}, {2: 4, 3: 8}, {1: 5}, {1: REMOVE}]
    )


def test_merge_nested_tsd():
    assert (
            eval_node(
                merge,
                tsl=[({1: {1: 1}, 2: {2: 2}}, {1: {1: 5}, 3: {3: 6}}), ({2: {2: 4}}, {3: {3: 8}}),
                     ({1: REMOVE, 2: {2: REMOVE}}, {}), ({}, {1: REMOVE})],
                resolution_dict={'tsl': TSL[TSD[int, TSD[int, TS[int]]], Size[2]]}
            ) == [
                {1: {1: 1}, 2: {2: 2}, 3: {3: 6}}, {2: {2: 4}, 3: {3: 8}}, {1: {1: 5}, 2: {2: REMOVE}}, {1: REMOVE}]
    )


def test_tsd_partition():
    @graph
    def g(ts: TSD[int, TS[int]], partitions: TSD[int, TS[str]]) -> TSD[str, TSD[int, TS[int]]]:
        return partition[K: int, K_1: str, TIME_SERIES_TYPE: TS[int]](ts, partitions)

    assert eval_node(g,
                     [{1: 1, 2: 2, 3: 3}, {1: 4, 2: 5, 3: 6}, {1: REMOVE}],
                     [{1: 'odd'}, {2: 'even', 3: 'odd'}, None, {2: REMOVE}, {3: 'prime'}]) == [
               {'odd': {1: 1}},
               {'odd': {1: 4, 3: 6}, 'even': {2: 5}},
               {'odd': {1: REMOVE}},
               {'even': {2: REMOVE}},
               {'prime': {3: 6}, 'odd': {3: REMOVE}}
           ]


def test_sub_tsds():
    @graph
    def app(tsd1: TSD[int, TS[int]], tsd2: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return tsd1 - tsd2

    assert eval_node(app,
                     [{1: 1}, {2: 2}],
                     [{2: 3}, {3: 2}]) \
           == [frozendict({1: 1}), None]


def test_sub_tsds_initial_lhs_valid_before_rhs():
    @graph
    def app(tsd1: TSD[int, TS[int]], tsd2: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return tsd1 - tsd2

    assert eval_node(app,
                     [{1: 1}, {2: 2}],
                     [None, {3: 2}]) \
           == [None, {1: 1, 2: 2}]


def test_bit_or_tsds():
    @graph
    def app(tsd1: TSD[int, TS[int]], tsd2: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return tsd1 | tsd2

    assert eval_node(app,
                     [{1: 1}, {2: 2}],
                     [{2: 3}, {3: 2}]) \
           == [frozendict({1: 1, 2: 3}), frozendict({2: 2, 3: 2})]


def test_bit_and_tsds():
    @graph
    def app(tsd1: TSD[int, TS[int]], tsd2: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return tsd1 & tsd2

    assert eval_node(app,
                     [{1: 1}, {2: 2}],
                     [{2: 3}, {3: 2}]) \
           == [None, frozendict({2: 2})]


def test_bit_xor_tsds():
    @graph
    def app(tsd1: TSD[int, TS[int]], tsd2: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return tsd1 ^ tsd2

    assert eval_node(app,
                     [{1: 1}, {2: 2}],
                     [{2: 3}, {3: 2}]) \
           == [frozendict({1: 1, 2: 3}), frozendict({3: 2, 2: REMOVE})]


@pytest.mark.xfail(
    reason="== does not work because it invokes __eq__ on WiringNodeInstance, which is an identity check.  _eq() works",
    strict=True)
def test_eq_tsds():
    @graph
    def app(tsd1: TSD[int, TS[int]], tsd2: TSD[int, TS[int]]) -> TS[bool]:
        # FIXME - == does not work because it invokes __eq__ on WiringNodeInstance which is
        # an id() equality
        if True:
            # This fails
            return tsd1 == tsd2
        else:
            # This works
            return eq_(tsd1, tsd2)

    assert eval_node(app,
                     [{1: 1}, {2: 2}],
                     [{2: 2}, {1: 1}]) \
           == [False, True]


@pytest.mark.parametrize(
    ['tp', 'expected', 'values'],
    [
        [TSD[int, TS[int]], [None, 1, 0], [{}, {0: 1}, {0: REMOVE}]],
    ]
)
def test_len_tsd(tp, expected, values):
    assert eval_node(len_, values, resolution_dict={'ts': tp}) == expected


def test_min_tsd_unary():
    @graph
    def app(tsd: TSD[int, TS[float]]) -> TS[float]:
        return min_(tsd)

    assert eval_node(app, [{3: 2.0}]) == [2.0]


def test_max_tsd_unary():
    @graph
    def app(tsd: TSD[int, TS[int]]) -> TS[int]:
        return max_(tsd)

    assert eval_node(app, [{3: 2, 100: -100}]) == [2]


@pytest.mark.xfail(reason="Empty TSDs don't seem to tick", strict=True)
def test_sum_tsd_unary():
    @graph
    def app(tsd: TSD[int, TS[int]]) -> TS[int]:
        from hgraph import log_
        log_("TSD {}", tsd)
        return sum_(tsd)

    assert eval_node(app, [frozendict({}), {3: 2, 1: 100}]) == [0, 102]


def test_str_tsd():
    @graph
    def app(tsd: TSD[int, TS[int]]) -> TS[str]:
        return str_(tsd)

    assert eval_node(app, [{3: 2}]) == ['{3: 2}']


def test_keys_as_tss():
    @graph
    def g(tsd: TSD[int, TS[int]]) -> TSS[int]:
        return keys_(tsd)

    assert eval_node(g, [{1: 1, 2: 2, 3: 3}, {1: REMOVE}]) == [{1, 2, 3}, {Removed(1)}]


def test_keys_as_set():
    @graph
    def g(tsd: TSD[int, TS[int]]) -> TS[Set[int]]:
        return keys_[OUT: TS[Set[int]]](tsd)

    assert eval_node(g, [{1: 1, 2: 2, 3: 3}, {1: REMOVE}]) == [{1, 2, 3}, {2, 3}]
