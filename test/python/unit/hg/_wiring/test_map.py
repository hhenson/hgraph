import pytest

from hg import graph, TS, TSD, TSS, TSL, SIZE, tsl_map, tsd_reduce, tsl_reduce
from hg import tsd_map
from hg.nodes import add_, debug_print
from hg.test import eval_node


@graph
def f_sum(key: TS[str], lhs: TS[int], rhs: TS[int]) -> TS[int]:
    a = add_(lhs, rhs)
    debug_print("key", key)
    debug_print("sum", a)
    return a


@pytest.mark.xfail(reason="Not implemented")
def test_tsd_map_wiring():
    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = tsd_map(f_sum, lhs=ts1, rhs=ts2)
        return m

    _test_tsd_map(map_test)


@pytest.mark.xfail(reason="Not implemented")
def test_tsd_map_wiring_no_key():
    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = tsd_map(add_, lhs=ts1, rhs=ts2)
        return m

    _test_tsd_map(map_test)


@pytest.mark.xfail(reason="Not implemented")
def test_tsd_map_wiring_no_key_no_kwargs():
    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = tsd_map(add_, ts1, ts2, keys=keys)
        return m

    _test_tsd_map(map_test)


@pytest.mark.xfail(reason="Not implemented")
def test_tsd_map_wiring_no_kwargs():

    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = tsd_map(add_, ts1, ts2, keys=keys)
        return m

    _test_tsd_map(map_test)


def _test_tsd_map(map_test):
    out = eval_node(map_test, [{'a', 'b'}, ], [{'a': 1}, {'b': 2}], [{'a': 2}, {'b': 3}])
    assert out == [{'a': 3}, {'b': 5}]


@pytest.mark.xfail(reason="Not implemented")
def test_tsl_map_wiring():

    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = tsd_map(f_sum, lhs=ts1, rhs=ts2)
        return m

    _test_tsl_map(map_test)


@pytest.mark.xfail(reason="Not implemented")
def test_tsl_map_wiring_no_key():
    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = tsd_map(add_, lhs=ts1, rhs=ts2)
        return m

    _test_tsl_map(map_test)


@pytest.mark.xfail(reason="Not implemented")
def test_tsl_map_wiring_no_key_no_kwargs():
    @graph
    def map_test(index: TSL[TS[bool], SIZE], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = tsd_map(add_, ts1, ts2, index=index)
        return m

    _test_tsl_map(map_test)


@pytest.mark.xfail(reason="Not implemented")
def test_tsl_map_wiring_no_kwargs():

    @graph
    def map_test(index: TSL[TS[bool], SIZE], ts1: TSL[TS[int], SIZE], ts2: TSL[TS[int], SIZE]) -> TSL[TS[int], SIZE]:
        m = tsl_map(add_, ts1, ts2, index=index)
        return m

    _test_tsl_map(map_test)


def _test_tsl_map(map_test):
    out = eval_node(map_test, [{0: True, 1: True}, ], [{0: 1}, {1: 2}], [{0: 2}, {1: 3}])
    assert out == [{0: 3}, {1: 5}]


@pytest.mark.xfail(reason="Not implemented")
def test_tsd_reduce():

    @graph
    def reduce_test(tsd: TSD[str, TS[int]]) -> TS[int]:
        return tsd_reduce(add_, tsd, 0)

    assert eval_node(reduce_test, [None, {'a': 1}, {'b': 2}]) == [0, 1, 3]


@pytest.mark.xfail(reason="Not implemented")
def test_tsl_reduce():

    @graph
    def reduce_test(tsl: TSL[TS[int], SIZE]) -> TS[int]:
        return tsl_reduce(add_, tsl, 0)

    assert eval_node(reduce_test, [None, {0: 1}, {1: 2}]) == [0, 1, 3]
