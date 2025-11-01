import math

import pytest

from hgraph import (
    TSS,
    graph,
    TS,
    Removed,
    not_,
    is_empty,
    set_delta,
    eq_,
    len_,
    and_,
    or_,
    min_,
    str_,
    max_,
    sum_,
    mean,
    std,
    var,
    TIME_SERIES_TYPE,
    contains_, compute_node, compute_set_delta,
)
from hgraph.arrow import eval_, arrow, assert_
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke

def test_is_empty():
    @graph
    def is_empty_test(tss: TSS[int]) -> TS[bool]:
        return is_empty(tss)

    assert eval_node(is_empty_test, [None, {1}, {2}, {Removed(1)}, {Removed(2)}]) == [True, False, None, None, True]


def test_not():
    @graph
    def is_empty_test(tss: TSS[int]) -> TS[bool]:
        return not_(tss)

    assert eval_node(is_empty_test, [None, {1}, {2}, {Removed(1)}, {Removed(2)}]) == [True, False, None, None, True]


def test_sub_tsss():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 - tss2

    assert eval_node(app, [{1}, {2}, None, None, {Removed(2)}], [{}, None, {1}, {1}, None]) == [
        {1},
        {2},
        {Removed(1)},
        None,
        {Removed(2)},
    ]


def test_sub_tsss_initial_lhs_valid_before_rhs():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 - tss2

    assert eval_node(app, [{1}, {2}], [None, {3}]) == [None, {1, 2}]


def test_bit_or_tsss():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 | tss2

    assert eval_node(
        app, [{1}, {2}, None, {4}, {5}, None, {Removed(5)}], [{1}, None, {3}, {5}, None, {Removed(5)}, None]
    ) == [{1}, {2}, {3}, {4, 5}, None, None, {Removed(5)}]


def test_bit_or_tsss_2():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 | tss2

    assert eval_node(
        app, [{1}, {2}, None, {4}, {5}, None, {Removed(5)}], [None, None, {3}, {5}, None, {Removed(5)}, None]
    ) == [{1}, {2}, {3}, {4, 5}, None, None, {Removed(5)}]


def test_bit_and_tsss():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 & tss2

    assert eval_node(
        app, [{1, 2, 3, 4}, {5, 6}, {Removed(1)}, {Removed(2)}, None], [{0, 2, 3, 5}, None, None, None, {-1, 1, 4}]
    ) == [{2, 3}, {5}, None, {Removed(2)}, {4}]


def test_bit_xor_tsss():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 ^ tss2

    assert eval_node(
        app, [{1, 2, 3, 4}, {5, 6}, {Removed(1)}, {Removed(2)}, None], [{0, 2, 3, 5}, None, None, None, {-1, 1, 4}]
    ) == [
               set_delta(added={0, 1, 4, 5}, removed=set(), tp=int),
               set_delta(removed={5}, added={6}, tp=int),
               set_delta(added=set(), removed={1}, tp=int),
               set_delta(added={2}, removed=set(), tp=int),
               set_delta(added={-1, 1}, removed={4}, tp=int),
           ]


def test_eq_tsss():
    assert eval_node(
        eq_[TIME_SERIES_TYPE: TSS[int]], [None, {1, 2}, {1, 2, 3}], [{1}, {4}, {1, 2, 3, Removed(4)}]
    ) == [
               None,
               False,
               True,
           ]


@pytest.mark.parametrize(
    ["tp", "expected", "values"],
    [
        [TSS[int], [0, 1, 3, 2], [{}, {1}, {2, 3}, {Removed(1)}]],
    ],
)
def test_len_tss(tp, expected, values):
    assert eval_node(len_, values, resolution_dict={"ts": tp}) == expected


def test_and_tsss():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TS[bool]:
        return and_(tss1, tss2)

    assert eval_node(app, [None, set(), {1, 2, 3}], [{1}, {4}, {1, 2, 3}]) == [None, False, True]


def test_or_tsss():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TS[bool]:
        return or_(tss1, tss2)

    assert eval_node(app, [None, set(), {1, 2, 3}], [{1}, {4}, {1, 2, 3}]) == [None, True, True]


def test_min_tss_unary():
    @graph
    def app(tss: TSS[int]) -> TS[int]:
        return min_(tss)

    assert eval_node(app, [None, set(), {1, 2, -1, 3}]) == [None, None, -1]


def test_min_tss_unary_default():
    @graph
    def app(tss: TSS[int]) -> TS[int]:
        return min_(tss, default_value=-1)

    assert eval_node(app, [set()]) == [-1]


def test_max_tss_unary():
    @graph
    def app(tss: TSS[int]) -> TS[int]:
        return max_(tss)

    assert eval_node(app, [None, set(), {1, 2, -1, 3}]) == [None, None, 3]


def test_max_tss_unary_default():
    @graph
    def app(tss: TSS[int]) -> TS[int]:
        return max_(tss, default_value=-1)

    assert eval_node(app, [set()]) == [-1]


def test_sum_tss_unary():
    @graph
    def app(tss: TSS[int]) -> TS[int]:
        return sum_(tss)

    assert eval_node(app, [set(), {1, 2, -1, 3}]) == [0, 5]


def test_mean_tss_unary():
    @graph
    def app(tss: TSS[int]) -> TS[float]:
        return mean(tss)

    output = eval_node(app, [set(), {1, 2, -1, 3}])
    assert math.isnan(output[0])
    assert output[1] == 1.25


def test_std_tss_unary():
    @graph
    def app(tss: TSS[int]) -> TS[float]:
        return std(tss)

    assert eval_node(app, [set(), {1}, {1, 2}, {1, 2, -1, 3}]) == [0.0, 0.0, 0.7071067811865476, 1.707825127659933]


def test_var_tss_unary():
    @graph
    def app(tss: TSS[int]) -> TS[float]:
        return var(tss)

    assert eval_node(app, [set(), {1}, {1, 2}, {1, 2, -1, 3}]) == [0.0, 0.0, 0.5, 2.9166666666666665]


def test_str_tss():
    @graph
    def app(tss: TSS[int]) -> TS[str]:
        return str_(tss)

    assert eval_node(app, [{1, 2, 3}]) == ["{1, 2, 3}"]


def test_contains_tss():
    @graph
    def g(tss: TSS[int], item: TS[int]) -> TS[bool]:
        return contains_(tss, item)

    assert eval_node(g, [{1, 2, 3}], [1, 4]) == [True, False]


def test_contains_tss_2():
    @graph
    def g(tss: TSS[int], item: TSS[int]) -> TS[bool]:
        return contains_(tss, item)

    assert eval_node(g, [{1, 2, 3}], [{1, 2}, {3, 4}]) == [True, False]


def test_add_tss_scalar():
    (
            eval_([{1, 2}], [3, 4], type_map=(TSS[int], TS[int]))
            | arrow(lambda p: p[0] + p[1])
            >> assert_(frozenset({1, 2, 3}), frozenset({1, 2, 3, 4}))
    )


def test_sub_tss_scalar():
    (
            eval_([{1, 2, 3, 4}], [3, 4], type_map=(TSS[int], TS[int]))
            | arrow(lambda p: p[0] - p[1])
            >> assert_(frozenset({1, 2, 4}), frozenset({1, 2}))
    )


def test_compute_set_delta():
    @compute_node
    def n(a: TS[frozenset[int]], _output: TSS[int] = None) -> TSS[int]:
        return compute_set_delta(a, _output)

    (
            eval_([frozenset({1, 2, 3}), frozenset({1, 2, }), frozenset({2, 3, 4})])
            | arrow(n)
            >> assert_(frozenset({1, 2, 3}), frozenset({1, 2}), frozenset({2, 3, 4}))
    )
