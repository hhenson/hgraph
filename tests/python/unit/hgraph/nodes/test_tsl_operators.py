from operator import invert

import pytest

from hgraph import Size, TS, TSL, MIN_TD, SIZE, TIME_SERIES_TYPE, add_, graph, sub_, mul_, eq_, ne_, neg_, pos_, abs_, \
    invert_, len_, min_, max_, sum_, str_, union_tsl, TSS, union
from hgraph._operators._control import merge
from hgraph.nodes import lag
from hgraph.nodes import tsl_to_tsd, index_of
from hgraph.test import eval_node


def test_merge():
    assert eval_node(merge, [None, 2, None, None, 6], [1, None, 4, None, None], [None, 3, 5, None, None],
                     resolution_dict={"tsl": TSL[TS[int], Size[3]]}) == [1, 2, 4, None, 6]


def test_tsl_lag():
    assert eval_node(lag, [{1: 1}, {0: 2, 2: 3}, {1: 4, 2: 5}, None, {0: 6}], MIN_TD,
                     resolution_dict={"ts": TSL[TS[int], Size[3]]}) == [None, {1: 1}, {0: 2, 2: 3}, {1: 4, 2: 5}, None,
                                                                        {0: 6}]


def test_add_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return add_(lhs, rhs)

    assert eval_node(app, [(1, 2)], [(2, 3)], resolution_dict={'ts': TS[int]}) == [{0:3, 1:5}]


def test_add_tsls_gaps():
    @graph
    def app(lhs: TSL[TS[int], Size[4]], rhs: TSL[TS[int], Size[4]]) -> TSL[TS[int], Size[4]]:
        return lhs + rhs

    assert eval_node(app, [(1, None, 2, 5)], [(2, 3, None, 6)], resolution_dict={'ts': TS[int]}) == [{0:3, 3:11}]


def test_sub_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return lhs - rhs

    assert eval_node(app, [(1, 2)], [(2, 3)], resolution_dict={'ts': TS[int]}) == [{0:-1, 1:-1}]


def test_mul_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return lhs * rhs

    assert eval_node(app, [(1, 2)], [(2, 3)], resolution_dict={'ts': TS[int]}) == [{0: 2, 1:6}]


def test_div_tsls():
    @graph
    def app(lhs: TSL[TS[float], Size[2]], rhs: TSL[TS[float], Size[2]]) -> TSL[TS[float], Size[2]]:
        return lhs / rhs

    assert eval_node(app, [(1.0, 2.0)], [(2.0, 1.0)], resolution_dict={'ts': TS[float]}) == [{0: 0.5, 1: 2.0}]


def test_floordiv_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs // rhs

    assert eval_node(app, [(3, 2, 100)], [(2, 2, 10)], resolution_dict={'ts': TS[int]}) == [{0: 1, 1: 1, 2: 10}]


def test_mod_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs % rhs

    assert eval_node(app, [(3, 2, 105)], [(2, 2, 10)], resolution_dict={'ts': TS[int]}) == [{0: 1, 1: 0, 2: 5}]


def test_pow_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs ** rhs

    assert eval_node(app, [(3, 4, 5)], [(3, 0, 1)], resolution_dict={'ts': TS[int]}) == [{0: 27, 1: 1, 2: 5}]


def test_lshift_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs << rhs

    assert eval_node(app, [(2, 10, 8)], [(1, 2, 3)], resolution_dict={'ts': TS[int]}) == [{0: 4, 1: 40, 2: 64}]


def test_rshift_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs >> rhs

    assert eval_node(app, [(2, 10, 1024)], [(1, 2, 3)], resolution_dict={'ts': TS[int]}) == [{0: 1, 1: 2, 2: 128}]


def test_bit_and_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs & rhs

    assert eval_node(app, [(2, 8, 7)], [(1, 9, 5)], resolution_dict={'ts': TS[int]}) == [{0: 0, 1: 8, 2: 5}]


def test_bit_or_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs | rhs

    assert eval_node(app, [(2, 8, 7)], [(1, 9, 5)], resolution_dict={'ts': TS[int]}) == [{0: 3, 1: 9, 2: 7}]


def test_bit_xor_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[3]], rhs: TSL[TS[int], Size[3]]) -> TSL[TS[int], Size[3]]:
        return lhs ^ rhs

    assert eval_node(app, [(2, 8, 7)], [(1, 9, 5)], resolution_dict={'ts': TS[int]}) == [{0: 3, 1: 1, 2: 2}]


def test_sum_tsls():
    @graph
    def app(lhs: TSL[TS[int], Size[2]], rhs: TSL[TS[int], Size[2]]) -> TSL[TS[int], Size[2]]:
        return sum_(lhs, rhs)

    assert eval_node(app, [(1, 2)], [(2, 3)]) == [{0:3, 1:5}]


def test_tsl_to_tsd():
    assert eval_node(tsl_to_tsd, [(1, 2, 3), {1: 3}], ('a', 'b', 'c'),
                     resolution_dict={'tsl': TSL[TS[int], Size[3]]}) == [{'a': 1, 'b': 2, 'c': 3}, {'b': 3}]


def test_index_of():
    assert eval_node(index_of[TIME_SERIES_TYPE: TS[int], SIZE: Size[3]],
                     [(1, 2, 3), None, (2, 3, 4), (-1, 0, 1)], [2, 1]) == [1, 0, -1, 2]


@pytest.mark.parametrize(
    ["lhs", "rhs", "expected"],
    [
        [(1, 2), (2, 3), False],
        [(1.0, 2.0), (2.0, 3.0), False],
        [(1.0, 2.0), (1.0, 2.0), True],
    ]
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
    ]
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
    ['tp', 'expected', 'values'],
    [
        [TSL[TS[int], Size[2]], [2, None, None], [{}, {0: 1}, {1: 2}]],
    ]
)
def test_len_tsl(tp, expected, values):
    assert eval_node(len_, values, resolution_dict={'ts': tp}) == expected


@pytest.mark.parametrize(
    ["lhs", "rhs", "expected"],
    [
        [(1, 2),     (2, 3),     {0: 1, 1: 2}],
        [(1.0, 2.0), (2.0, 3.0), {0: 1.0, 1: 2.0}],
    ]
)
def test_min_tsls(lhs, rhs, expected):
    tp = type(lhs[0])
    @graph
    def g(lhs: TSL[TS[tp], Size[2]], rhs: TSL[TS[tp], Size[2]]) -> TSL[TS[tp], Size[2]]:
        return min_(lhs, rhs)

    assert eval_node(g, [lhs], [rhs]) == [expected]


@pytest.mark.parametrize(
    ["lhs", "rhs", "expected"],
    [
        [(1, 2),     (2, 3),     {0: 2, 1: 3}],
        [(1.0, 2.0), (2.0, 3.0), {0: 2.0, 1: 3.0}],
    ]
)
def test_max_tsls(lhs, rhs, expected):
    tp = type(lhs[0])
    @graph
    def g(lhs: TSL[TS[tp], Size[2]], rhs: TSL[TS[tp], Size[2]]) -> TSL[TS[tp], Size[2]]:
        return max_(lhs, rhs)

    assert eval_node(g, [lhs], [rhs]) == [expected]


def test_min_tsl_unary():
    @graph
    def g(ts: TSL[TS[int], Size[5]]) -> TS[int]:
        return min_(ts)

    assert eval_node(g, [(3, 5, 2, 8, 10)]) == [2]


def test_max_tsl_unary():
    @graph
    def g(ts: TSL[TS[int], Size[5]]) -> TS[int]:
        return max_(ts)

    assert eval_node(g, [(3, 5, 10, 2, 8)]) == [10]


def test_sum_tsl_unary():
    @graph
    def g(ts: TSL[TS[int], Size[5]]) -> TS[int]:
        return sum_(ts)

    assert eval_node(g, [(3, 5, 2, 8, 10)]) == [28]


def test_str_tsl():
    @graph
    def g(ts: TSL[TS[int], Size[5]]) -> TS[str]:
        return str_(ts)

    assert eval_node(g, [(3, 5, 2, 8, 10)]) == ['(3, 5, 2, 8, 10)']


def test_union_tsl_unary():
    @graph
    def app(ts: TSL[TSS[int], Size[3]]) -> TSS[int]:
        return union_tsl(ts)

    assert eval_node(app, [({1, 2, 3}, {3, 4, 5}, {4, 5, 6})]) == [{1, 2, 3, 4, 5, 6}]
