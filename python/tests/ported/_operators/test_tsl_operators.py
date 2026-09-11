import pytest

from hgraph import (
    Size,
    TS,
    TSL,
    MIN_TD,
    SIZE,
    TIME_SERIES_TYPE,
    add_,
    graph,
    eq_,
    ne_,
    neg_,
    pos_,
    abs_,
    invert_,
    len_,
    min_,
    max_,
    sum_,
    str_,
    lag,
    mean,
    index_of,
    TIME_SERIES_TYPE_2,
    TIME_SERIES_TYPE_1,
)
from hgraph.nodes import tsl_to_tsd
from hgraph.test import eval_node





def test_tsl_lag():
    assert eval_node(
        lag, [{1: 1}, {0: 2, 2: 3}, {1: 4, 2: 5}, None, {0: 6}], MIN_TD, resolution_dict={"ts": TSL[TS[int], Size[3]]}
    ) == [None, {1: 1}, {0: 2, 2: 3}, {1: 4, 2: 5}, None, {0: 6}]


def test_add_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return add_(lhs, rhs)

    assert eval_node(app, [(1, 2)], [(2, 3)]) == [{0: 3, 1: 5}]


def test_add_tsls_gaps():
    @graph
    def app(lhs: TSL[TS[int], Size[4]], rhs: TSL[TS[int], Size[4]]) -> TSL[TS[int], Size[4]]:
        return lhs + rhs

    assert eval_node(app, [(1, None, 2, 5)], [(2, 3, None, 6)]) == [{0: 3, 3: 11}]


def test_sub_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return lhs - rhs

    assert eval_node(app, [(1, 2)], [(2, 3)]) == [{0: -1, 1: -1}]


def test_mul_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return lhs * rhs

    assert eval_node(app, [(1, 2)], [(2, 3)]) == [{0: 2, 1: 6}]


def test_mul_tsl_scalar():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TS[int]) -> TSL[TS[int], Size[2]]:
        return lhs * rhs

    assert eval_node(app, [(1, 2)], [2]) == [{0: 2, 1: 4}]


def test_div_tsls():
    @graph
    def app(lhs: TSL[TS[float], Size[2]], rhs: TSL[TS[float], Size[2]]) -> TSL[TS[float], Size[2]]:
        return lhs / rhs

    assert eval_node(app, [(1.0, 2.0)], [(2.0, 1.0)]) == [{0: 0.5, 1: 2.0}]


def test_div_tsl_scalar():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TS[int]) -> TSL[TS[float], Size[2]]:
        return lhs / rhs

    assert eval_node(app, [(1, 2)], [2]) == [{0: 0.5, 1: 1.0}]


def test_floordiv_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs // rhs

    assert eval_node(app, [(3, 2, 100)], [(2, 2, 10)]) == [{0: 1, 1: 1, 2: 10}]


def test_floordiv_tsl_scalar():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TS[int]) -> TSL[TS[int], Size[2]]:
        return lhs // rhs

    assert eval_node(app, [(1, 2)], [2]) == [{0: 0, 1: 1}]


def test_mod_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs % rhs

    assert eval_node(app, [(3, 2, 105)], [(2, 2, 10)]) == [{0: 1, 1: 0, 2: 5}]


def test_mod_tsl_scalar():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TS[int]) -> TSL[TS[int], Size[2]]:
        return lhs % rhs

    assert eval_node(app, [(3, 2)], [2]) == [{0: 1, 1: 0}]


def test_pow_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs**rhs

    assert eval_node(app, [(3, 4, 5)], [(3, 0, 1)]) == [{0: 27, 1: 1, 2: 5}]


def test_pow_tsl_scalar():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TS[int]) -> TSL[TS[int], Size[2]]:
        return lhs**rhs

    assert eval_node(app, [(3, 2)], [2]) == [{0: 9, 1: 4}]


def test_lshift_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs << rhs

    assert eval_node(app, [(2, 10, 8)], [(1, 2, 3)]) == [{0: 4, 1: 40, 2: 64}]


def test_rshift_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs >> rhs

    assert eval_node(app, [(2, 10, 1024)], [(1, 2, 3)]) == [{0: 1, 1: 2, 2: 128}]


def test_bit_and_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs & rhs

    assert eval_node(app, [(2, 8, 7)], [(1, 9, 5)]) == [{0: 0, 1: 8, 2: 5}]


def test_bit_or_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs | rhs

    assert eval_node(app, [(2, 8, 7)], [(1, 9, 5)]) == [{0: 3, 1: 9, 2: 7}]


def test_bit_xor_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs ^ rhs

    assert eval_node(app, [(2, 8, 7)], [(1, 9, 5)]) == [{0: 3, 1: 1, 2: 2}]


@pytest.mark.parametrize(
    ["tsl", "expected"],
    [
        [(20,), 20],
        [(20, 30), 50],
        [(3, 5, 2, 8, 10), 28],
    ],
)
def test_sum_tsl_unary(tsl, expected):
    @graph
    def g(tsl: TSL[TS[int], Size[len(tsl)]]) -> TS[int]:
        return sum_(tsl)

    assert eval_node(g, [tsl]) == [expected]


@pytest.mark.parametrize(
    ["lhs", "rhs", "expected"],
    [
        [(1, 2), (2, 3), {0: 3, 1: 5}],
        [(1.0, 2.0), (2.0, 3.0), {0: 3.0, 1: 5.0}],
    ],
)
def test_sum_tsls_multi(lhs, rhs, expected):
    tp = type(lhs[0])

    @graph
    def g(lhs: TSL[TS[tp], Size[2]], rhs: TSL[TS[tp], Size[2]]) -> TSL[TS[tp], Size[2]]:
        return sum_(lhs, rhs)

    assert eval_node(g, [lhs], [rhs]) == [expected]


@pytest.mark.parametrize(
    ["tsl", "expected"],
    [
        [(20,), 20],
        [(20, 30), 25],
        [(3, 5, 2, 8, 10), 5.6],
    ],
)
def test_mean_tsl_unary(tsl, expected):
    @graph
    def g(tsl: TSL[TS[int], Size[len(tsl)]]) -> TS[float]:
        return mean(tsl)

    assert eval_node(g, [tsl]) == [expected]


@pytest.mark.parametrize(
    ["lhs", "rhs", "expected"],
    [
        [(1, 2), (2, 3), {0: 1.5, 1: 2.5}],
        [(1.0, 2.0), (2.0, 3.0), {0: 1.5, 1: 2.5}],
    ],
)
def test_mean_tsls_multi(lhs, rhs, expected):
    tp = type(lhs[0])

    @graph
    def g(lhs: TSL[TS[tp], Size[2]], rhs: TSL[TS[tp], Size[2]]) -> TSL[TS[float], Size[2]]:
        return mean(lhs, rhs)

    assert eval_node(g, [lhs], [rhs]) == [expected]


def test_tsl_to_tsd():
    assert eval_node(
        tsl_to_tsd, [(1, 2, 3), {1: 3}], ("a", "b", "c"), resolution_dict={"tsl": TSL[TS[int], Size[3]]}
    ) == [{"a": 1, "b": 2, "c": 3}, {"b": 3}]


def test_index_of():
    assert eval_node(
        index_of[TIME_SERIES_TYPE_1 : TSL[TS[int], Size[3]], TIME_SERIES_TYPE_2 : TS[int]],
        [(1, 2, 3), None, (2, 3, 4), (-1, 0, 1)],
        [2, 1],
    ) == [1, 0, -1, 2]


def test_index_of_waits_for_a_list_with_something_in_it():
    """``index_of`` must not answer "-1, not found" for a list it has not
    received anything to search yet.

    The list input is validity-checked: a collection is valid once at least
    one child has a value, so a partly-filled list still searches, but an
    entirely empty one produces nothing at all. Answering -1 there put a
    "not found" on the wire a cycle before released hgraph answers anything
    (parity #861).
    """

    @graph
    def g(a: TS[int], b: TS[int], item: TS[int]) -> TS[int]:
        return index_of(TSL.from_ts(a, b), item)

    # Nothing to search: no answer at all, not "-1". eval_node collapses a
    # trace with no ticks in it to None.
    assert eval_node(g, [None, None], [None, None], [None, 0]) is None

    # The item arrives first; the answer waits for the list, then says -1.
    assert eval_node(g, [None, 5], [None, 7], [3, None]) == [None, -1]


def test_index_of_searches_a_partly_valid_list():
    """One valid child is enough to search -- the gaps are skipped, not
    waited for."""

    @graph
    def g(a: TS[int], b: TS[int], item: TS[int]) -> TS[int]:
        return index_of(TSL.from_ts(a, b), item)

    assert eval_node(g, [None, 5], [None, None], [None, 5]) == [None, 0]
    assert eval_node(g, [None, 5], [None, None], [None, 9]) == [None, -1]


@pytest.mark.parametrize(
    ["lhs", "rhs", "expected"],
    [
        [(1, 2), (2, 3), False],
        [(1.0, 2.0), (2.0, 3.0), False],
        [(1.0, 2.0), (1.0, 2.0), True],
    ],
)
def test_eq_tsls(lhs, rhs, expected):
    tp = type(lhs[0])

    @graph
    def g(lhs: TSL[TS[tp], Size[2]], rhs: TSL[TS[tp], Size[2]]) -> TS[bool]:
        return eq_(lhs, rhs)

    assert eval_node(g, [lhs], [rhs]) == [expected]


@pytest.mark.parametrize(
    ["lhs", "rhs", "expected"],
    [
        [(1, 2), (2, 3), True],
        [(1.0, 2.0), (2.0, 3.0), True],
        [(1.0, 2.0), (1.0, 2.0), False],
    ],
)
def test_ne_tsl(lhs, rhs, expected):
    tp = type(lhs[0])

    @graph
    def g(lhs: TSL[TS[tp], Size[2]], rhs: TSL[TS[tp], Size[2]]) -> TS[bool]:
        return ne_(lhs, rhs)

    assert eval_node(g, [lhs], [rhs]) == [expected]


def test_neg_tsl():
    @graph
    def g(ts: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return neg_(ts)

    assert eval_node(g, [(1, 2)]) == [{0: -1, 1: -2}]


def test_pos_tsl():
    @graph
    def g(ts: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return pos_(ts)

    assert eval_node(g, [(1, -2)]) == [{0: 1, 1: -2}]


def test_abs_tsl():
    @graph
    def g(ts: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return abs_(ts)

    assert eval_node(g, [(1, -2)]) == [{0: 1, 1: 2}]


def test_invert_tsl():
    @graph
    def g(ts: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return invert_(ts)

    assert eval_node(g, [(1, -2)]) == [{0: -2, 1: 1}]


@pytest.mark.parametrize(
    ["tp", "expected", "values"],
    [
        [TSL[TS[int], Size[2]], [2, None, None], [{}, {0: 1}, {1: 2}]],
    ],
)
def test_len_tsl(tp, expected, values):
    assert eval_node(len_, values, resolution_dict={"ts": tp}) == expected


@pytest.mark.parametrize(
    ["tsl", "expected"],
    [
        [(20,), 20],
        [(18, 20), 18],
        [(20, 18), 18],
        [(250, 20, 30), 20],
        [(3, 2, 8, 10), 2],
        [(3, 5, 2, 8, 10), 2],
    ],
)
def test_min_tsl_unary(tsl, expected):
    @graph
    def g(ts: TSL[TS[int], Size[len(tsl)]]) -> TS[int]:
        return min_(ts)

    assert eval_node(g, [tsl]) == [expected]


@pytest.mark.parametrize(
    ["lhs", "rhs", "expected"],
    [
        [(1, 2), (2, 3), {0: 1, 1: 2}],
        [(1.0, 2.0), (2.0, 3.0), {0: 1.0, 1: 2.0}],
    ],
)
def test_min_tsls_multi(lhs, rhs, expected):
    tp = type(lhs[0])

    @graph
    def g(lhs: TSL[TS[tp], Size[2]], rhs: TSL[TS[tp], Size[2]]) -> TSL[TS[tp], Size[2]]:
        return min_(lhs, rhs)

    assert eval_node(g, [lhs], [rhs]) == [expected]


@pytest.mark.parametrize(
    ["lhs", "rhs", "expected"],
    [
        [(1, 2), (2, 3), {0: 2, 1: 3}],
        [(1.0, 2.0), (2.0, 3.0), {0: 2.0, 1: 3.0}],
    ],
)
def test_max_tsls_multi(lhs, rhs, expected):
    tp = type(lhs[0])

    @graph
    def g(lhs: TSL[TS[tp], Size[2]], rhs: TSL[TS[tp], Size[2]]) -> TSL[TS[tp], Size[2]]:
        return max_(lhs, rhs)

    assert eval_node(g, [lhs], [rhs]) == [expected]


@pytest.mark.parametrize(
    ["tsl", "expected"],
    [
        [(20,), 20],
        [(250, 20, 30), 250],
        [(3, 5, 2, 8, 10), 10],
    ],
)
def test_max_tsl_unary(tsl, expected):
    @graph
    def g(ts: TSL[TS[int], Size[len(tsl)]]) -> TS[int]:
        return max_(ts)

    assert eval_node(g, [tsl]) == [expected]


def test_str_tsl():
    @graph
    def g(ts: TSL[TS[int], Size[5]]) -> TS[str]:
        return str_(ts)

    assert eval_node(g, [(3, 5, 2, 8, 10)]) == ["(3, 5, 2, 8, 10)"]
