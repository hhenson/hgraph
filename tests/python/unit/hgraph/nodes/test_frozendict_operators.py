import math

import pytest
from frozendict import frozendict

from hgraph import sub_, getitem_, TS, and_, graph, KEYABLE_SCALAR, SCALAR, or_, min_, max_, sum_, str_, WiringError, \
    mean, std, var
from hgraph.test import eval_node


def test_sub_frozendicts():
    assert eval_node(sub_, [frozendict({1: 10, 2: 20})], [frozendict({2: 25, 3: 30})]) == [frozendict({1: 10})]


def test_getitem_frozendict():
    assert eval_node(getitem_, [frozendict({1: 10, 2: 20})], [2]) == [20]


def test_and_frozendicts():
    @graph
    def app(lhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]], rhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[bool]:
        return and_(lhs, rhs)

    assert eval_node(app, [frozendict({1: 2}), frozendict()],
                          [frozendict({3: 4}), frozendict({3: 4})]) == [True, False]


def test_or_frozendicts():
    @graph
    def app(lhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]], rhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[bool]:
        return or_(lhs, rhs)

    assert eval_node(app, [frozendict({1: 2}), frozendict()],
                          [frozendict({3: 4}), frozendict({3: 4})]) == [True, True]


def test_min_frozendict_unary():
    @graph
    def app(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[SCALAR]:
        return min_(ts)

    assert eval_node(app, [frozendict({1: 10, 2: 20})]) == [10]


def test_min_frozendict_unary_default():
    @graph
    def app(ts: TS[frozendict[int, int]], default_value: TS[int]) -> TS[int]:
        return min_(ts, default_value=default_value)

    assert eval_node(app, [frozendict({})], [-1]) == [-1]


def test_min_frozendict_multi():
    @graph
    def app(ts1: TS[frozendict[KEYABLE_SCALAR, SCALAR]],
            ts2: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[SCALAR]:
        return min_(ts2, ts2)

    with pytest.raises(WiringError) as e:
        eval_node(app, [frozendict({1: 10, 2: 20})], [frozendict({99: 99})])
    assert "Cannot compute min of 2 frozendicts" in str(e)


def test_max_frozendict_unary():
    @graph
    def app(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[SCALAR]:
        return max_(ts)

    assert eval_node(app, [frozendict({1: 10, 2: 20})]) == [20]


def test_max_frozendict_unary_default():
    @graph
    def app(ts: TS[frozendict[int, int]], default_value: TS[int]) -> TS[int]:
        return max_(ts, default_value=default_value)

    assert eval_node(app, [frozendict({})], [-1]) == [-1]


def test_sum_frozendict_unary():
    @graph
    def app(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[KEYABLE_SCALAR]:
        return sum_(ts)

    assert eval_node(app, [frozendict({1: 10, 2: 20})]) == [30]


def test_sum_frozendict_unary_default():
    @graph
    def app(ts: TS[frozendict[int, float]]) -> TS[float]:
        return sum_(ts)

    assert eval_node(app, [frozendict({})]) == [0.0]


def test_mean_frozendict_unary_int():
    @graph
    def app(ts: TS[frozendict[int, int]]) -> TS[float]:
        return mean(ts)

    assert eval_node(app, [frozendict({1: 10, 2: 20})]) == [15.0]


def test_mean_frozendict_unary_float():
    @graph
    def app(ts: TS[frozendict[int, float]]) -> TS[float]:
        return mean(ts)

    assert eval_node(app, [frozendict({1: 10.0, 2: 20.0}), frozendict({1: 10.0})]) == [15.0, 10.0]


def test_mean_frozendict_unary_default():
    @graph
    def app(ts: TS[frozendict[int, float]]) -> TS[float]:
        return mean(ts)

    out = eval_node(app, [frozendict({})])[0]
    assert math.isnan(out)


def test_std_frozendict_unary():
    @graph
    def app(ts: TS[frozendict[int, int]]) -> TS[float]:
        return std(ts)

    assert eval_node(app, [frozendict(), frozendict({1: 1}), frozendict({1: 10, 2: 20}), frozendict({1: 10, 2: 20, 3: 20})]) == [0.0, 0.0, 7.0710678118654755, 5.773502691896257]


def test_var_frozendict_unary():
    @graph
    def app(ts: TS[frozendict[int, int]]) -> TS[float]:
        return var(ts)

    assert eval_node(app, [frozendict(), frozendict({1: 1}), frozendict({1: 10, 2: 20}), frozendict({1: 10, 2: 20, 3: 20})]) == [0.0, 0.0, 50.0, 33.333333333333336]


def test_str_frozendict():
    @graph
    def app(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[str]:
        return str_(ts)

    assert eval_node(app, [frozendict({1: 10, 2: 20})]) == ['{1: 10, 2: 20}']
