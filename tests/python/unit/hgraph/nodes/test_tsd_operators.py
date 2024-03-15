import pytest
from frozendict import frozendict

from hgraph import TS, graph, TIME_SERIES_TYPE, SCALAR_2, TSD, REMOVE, not_, SCALAR, K, TimeSeriesSchema, TSB, compute_node, REF, TSS
from hgraph.nodes import make_tsd, extract_tsd, flatten_tsd, is_empty, sum_, tsd_get_item, const
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
