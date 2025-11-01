import pytest

from hgraph import (
    TimeSeriesSchema,
    TS,
    TSB,
    graph,
    compute_node,
    TIME_SERIES_TYPE,
    REF,
    WiringError,
    eq_,
    abs_,
    min_,
    max_,
    sum_,
    mean,
    std,
)
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke

def test_tsb_get_item():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    @compute_node
    def make_ref(r: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
        return r.value

    @graph
    def g(b: TSB[ABSchema]) -> TSB[ABSchema]:
        br = make_ref(b)
        return {"a": br.a + 1, "b": br["b"]}

    assert eval_node(g, [{"a": 1, "b": "2"}]) == [{"a": 2, "b": "2"}]


def test_tsb_get_item_by_index():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    @compute_node
    def make_ref(r: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
        return r.value

    @graph
    def g(b: TSB[ABSchema]) -> TSB[ABSchema]:
        br = make_ref(b)
        return {"a": br[0] + 1, "b": br[1]}

    assert eval_node(g, [{"a": 1, "b": "2"}]) == [{"a": 2, "b": "2"}]


def test_add_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs + rhs

    assert eval_node(g, [{"a": 1, "b": "2"}], [{"a": 3, "b": "4"}]) == [{"a": 4, "b": "24"}]


def test_sub_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs - rhs

    assert eval_node(g, [{"a": 1, "b": 2}], [{"a": 3, "b": 4}]) == [{"a": -2, "b": -2}]


def test_sub_tsbs_invalid():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs - rhs

    with pytest.raises(WiringError) as e:
        eval_node(g, [{"a": 1, "b": 2}], [{"a": 3, "b": 4}])

    assert "Cannot subtract one string from another" in str(e)


def test_mul_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[float]
        b: TS[int]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs * rhs

    assert eval_node(g, [{"a": 1.0, "b": 2}], [{"a": 3.5, "b": 4}]) == [{"a": 3.5, "b": 8}]


def test_div_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[float]
        b: TS[float]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs / rhs

    assert eval_node(g, [{"a": 1.0, "b": 4.0}], [{"a": 0.5, "b": 2.0}]) == [{"a": 2.0, "b": 2.0}]


def test_floordiv_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs // rhs

    assert eval_node(g, [{"a": 1, "b": 3}], [{"a": 2, "b": 2}]) == [{"a": 0, "b": 1}]


def test_pow_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs**rhs

    assert eval_node(g, [{"a": 1, "b": 3}], [{"a": 2, "b": 2}]) == [{"a": 1, "b": 9}]


def test_lshift_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[float]
        b: TS[int]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs**rhs

    assert eval_node(g, [{"a": 1.0, "b": 2}], [{"a": 2.0, "b": 3}]) == [{"a": 1.0, "b": 8}]


def test_rshift_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs >> rhs

    assert eval_node(g, [{"a": 100, "b": 256}], [{"a": 2, "b": 3}]) == [{"a": 25, "b": 32}]


def test_bit_and_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs & rhs

    assert eval_node(g, [{"a": 7, "b": 8}], [{"a": 5, "b": 9}]) == [{"a": 5, "b": 8}]


def test_bit_or_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs | rhs

    assert eval_node(g, [{"a": 7, "b": 8}], [{"a": 5, "b": 9}]) == [{"a": 7, "b": 9}]


def test_bit_xor_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return lhs ^ rhs

    assert eval_node(g, [{"a": 7, "b": 8}], [{"a": 5, "b": 9}]) == [{"a": 2, "b": 1}]


def test_eq_tsbs():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TS[bool]:
        return eq_(lhs, rhs)

    assert eval_node(g, [{"a": 7, "b": 8}, {"a": 1, "b": 100}], [{"a": 5, "b": 9}, {"a": 1, "b": 100}]) == [False, True]


def test_neg_tsb():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[float]

    @graph
    def g(tsb: TSB[ABSchema]) -> TSB[ABSchema]:
        return -tsb

    assert eval_node(g, [{"a": 7, "b": -8.0}]) == [{"a": -7, "b": 8.0}]


def test_pos_tsb():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[float]

    @graph
    def g(tsb: TSB[ABSchema]) -> TSB[ABSchema]:
        return +tsb

    assert eval_node(g, [{"a": 7, "b": -8.0}]) == [{"a": 7, "b": -8.0}]


def test_invert_tsb():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(tsb: TSB[ABSchema]) -> TSB[ABSchema]:
        return ~tsb

    assert eval_node(g, [{"a": 1, "b": 12}]) == [{"a": -2, "b": -13}]


def test_abs_tsb():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[float]

    @graph
    def g(tsb: TSB[ABSchema]) -> TSB[ABSchema]:
        return abs_(tsb)

    assert eval_node(g, [{"a": -1, "b": -2.5}]) == [{"a": 1, "b": 2.5}]


def test_eq_tsb_different_bundles():
    class B1(TimeSeriesSchema):
        a: TS[int]

    class B2(TimeSeriesSchema):
        A: TS[int]

    @graph
    def g(lhs: TSB[B1], rhs: TSB[B2]) -> TS[bool]:
        return eq_(lhs, rhs)

    assert eval_node(
        g,
        [
            {"a": 7},
        ],
        [{"A": 7}],
    ) == [False]


def test_min_tsbs_multi():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return min_(lhs, rhs)

    assert eval_node(g, [{"a": 7, "b": "8"}, {"a": 1, "b": "100"}], [{"a": 5, "b": "9"}, {"a": 1, "b": "100"}]) == [
        {"a": 5, "b": "8"},
        {"a": 1, "b": "100"},
    ]


def test_max_tsbs_multi():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return max_(lhs, rhs)

    assert eval_node(g, [{"a": 7, "b": "8"}, {"a": 1, "b": "100"}], [{"a": 5, "b": "9"}, {"a": 1, "b": "100"}]) == [
        {"a": 7, "b": "9"},
        {"a": 1, "b": "100"},
    ]


def test_min_tsb_unary():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]
        c: TS[int]

    @graph
    def g(tsb: TSB[ABSchema]) -> TS[int]:
        return min_(tsb)

    assert (eval_node(g, [{"a": 7, "b": 8, "c": 3}])) == [3]


def test_min_tsb_unary_single_column():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]

    @graph
    def g(tsb: TSB[ABSchema]) -> TS[int]:
        return min_(tsb)

    assert (eval_node(g, [{"a": 7}])) == [7]


def test_max_tsb_unary():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]
        c: TS[int]

    @graph
    def g(ts: TSB[ABSchema]) -> TS[int]:
        return max_(ts)

    assert (eval_node(g, [{"a": 7, "b": 8, "c": 3}])) == [8]


def test_sum_tsb_unary():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]
        c: TS[int]

    @graph
    def g(ts: TSB[ABSchema]) -> TS[int]:
        return sum_(ts)

    assert (eval_node(g, [{"a": 7, "b": 8, "c": 3}])) == [18]


def test_sum_tsbs_multi():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return sum_(*(lhs, rhs))

    assert eval_node(g, [{"a": 7, "b": "8"}, {"a": 1, "b": "100"}], [{"a": 5, "b": "9"}, {"a": 1, "b": "100"}]) == [
        {"a": 12, "b": "89"},
        {"a": 2, "b": "100100"},
    ]


def test_mean_tsb_unary():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]
        c: TS[int]

    @graph
    def g(ts: TSB[ABSchema]) -> TS[float]:
        return mean(ts)

    assert (eval_node(g, [{"a": 7, "b": 8, "c": 9}])) == [8.0]


def test_mean_tsbs_multi():
    class ABSchema(TimeSeriesSchema):
        a: TS[float]
        b: TS[float]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return mean(*(lhs, rhs))

    assert eval_node(
        g, [{"a": 7.0, "b": 8.0}, {"a": 1.0, "b": 100.0}], [{"a": 5.0, "b": 9.0}, {"a": 1.0, "b": 100.0}]
    ) == [{"a": 6.0, "b": 8.5}, {"a": 1.0, "b": 100.0}]


def test_std_tsb_unary():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]
        c: TS[int]

    @graph
    def g(ts: TSB[ABSchema]) -> TS[float]:
        return std(ts)

    assert (eval_node(g, [{"a": 70, "b": 80, "c": 90}])) == [10.0]


def test_std_tsbs_multi():
    class ABSchema(TimeSeriesSchema):
        a: TS[float]
        b: TS[float]

    @graph
    def g(lhs: TSB[ABSchema], rhs: TSB[ABSchema]) -> TSB[ABSchema]:
        return std(*(lhs, rhs))

    assert eval_node(
        g, [{"a": 7.0, "b": 8.0}, {"a": 1.0, "b": 100.0}], [{"a": 5.0, "b": 9.0}, {"a": 1.0, "b": 100.0}]
    ) == [{"a": 1.4142135623730951, "b": 0.7071067811865476}, {"a": 0.0, "b": 0.0}]
