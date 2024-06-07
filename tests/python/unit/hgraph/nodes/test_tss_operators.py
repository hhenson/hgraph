import pytest

from hgraph import TSS, graph, TS, Removed, not_, is_empty, SetDelta, PythonSetDelta, eq_, len_, and_, or_, min_, str_, \
    union, max_, sum_
from hgraph.test import eval_node


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

    assert eval_node(app,
                     [{1}, {2},  None,         None, {Removed(2)}],
                     [{},  None, {1},          {1},  None]) \
           ==        [{1}, {2},  {Removed(1)}, None, {Removed(2)}]


def test_sub_tsss_initial_lhs_valid_before_rhs():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 - tss2

    assert eval_node(app,
                     [{1},  {2}],
                     [None, {3}]) \
           ==        [None, {1, 2} ]


def test_bit_or_tsss():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 | tss2

    assert eval_node(app,
                     [{1}, {2},  None, {4},    {5},  None,         {Removed(5)}],
                     [{1}, None, {3},  {5},    None, {Removed(5)}, None]) \
           ==        [{1}, {2},  {3},  {4, 5}, None, None,         {Removed(5)}]


def test_bit_and_tsss():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 & tss2

    assert eval_node(app,
                     [{1, 2, 3, 4}, {5, 6}, {Removed(1)}, {Removed(2)},     None],
                     [{0, 2, 3, 5}, None,   None,         None,             {-1, 1, 4}]) \
           ==        [{2, 3},       {5},    None,         {Removed(2)},     {4}]


def test_bit_xor_tsss():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TSS[int]:
        return tss1 ^ tss2

    assert eval_node(app,
             [{1, 2, 3, 4},                                         {5, 6},                                    {Removed(1)},                              {Removed(2)},                             None],
             [{0, 2, 3, 5},                                         None,                                      None,                                      None,                                     {-1, 1, 4}]) \
   ==        [PythonSetDelta(added={0, 1, 4, 5}, removed=set()),    PythonSetDelta(removed={5}, added={6}),    PythonSetDelta(added=set(), removed={1}),  PythonSetDelta(added={2}, removed=set()), PythonSetDelta(added={-1, 1}, removed={4})]


def test_eq_tsss():
    assert eval_node(eq_, [None, {1, 2},    {1, 2, 3}],
                          [{1},  {4},       {1, 2, 3}]) == \
                          [None, False,     True]


@pytest.mark.parametrize(
    ['tp', 'expected', 'values'],
    [
        [TSS[int], [0, 1, 3, 2], [{}, {1}, {2, 3}, {Removed(1)}]],
    ]
)
def test_len_tss(tp, expected, values):
    assert eval_node(len_, values, resolution_dict={'ts': tp}) == expected


def test_and_tsss():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TS[bool]:
        return and_(tss1, tss2)

    assert eval_node(app, [None, set(),    {1, 2, 3}],
                          [{1},  {4},   {1, 2, 3}]) == \
                          [None, False, True]


def test_or_tsss():
    @graph
    def app(tss1: TSS[int], tss2: TSS[int]) -> TS[bool]:
        return or_(tss1, tss2)

    assert eval_node(app, [None, set(),    {1, 2, 3}],
                          [{1},  {4},   {1, 2, 3}]) == \
                          [None, True, True]


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


def test_str_tss():
    @graph
    def app(tss: TSS[int]) -> TS[str]:
        return str_(tss)

    assert eval_node(app, [{1, 2, 3}]) == ["{1, 2, 3}"]


def test_union_tss():
    @graph
    def app(ts1: TSS[int], ts2: TSS[int], ts3: TSS[int]) -> TSS[int]:
        return union(ts1, ts2, ts3)

    assert eval_node(app, [{1, 2, 3}], [{3, 4, 5}], [{4, 5, 6}]) == [{1, 2, 3, 4, 5, 6}]
