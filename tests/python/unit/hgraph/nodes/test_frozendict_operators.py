from frozendict import frozendict

from hgraph import sub_, getitem_, TS, and_, graph, KEYABLE_SCALAR, SCALAR, or_, min_, max_, sum_, str_
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
        return min_(ts, default_value)

    assert eval_node(app, [frozendict({})], [-1]) == [-1]


def test_max_frozendict_unary():
    @graph
    def app(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[SCALAR]:
        return max_(ts)

    assert eval_node(app, [frozendict({1: 10, 2: 20})]) == [20]


def test_max_frozendict_unary_default():
    @graph
    def app(ts: TS[frozendict[int, int]], default_value: TS[int]) -> TS[int]:
        return max_(ts, default_value)

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


def test_str_frozendict():
    @graph
    def app(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[str]:
        return str_(ts)

    assert eval_node(app, [frozendict({1: 10, 2: 20})]) == ['{1: 10, 2: 20}']
