import pytest
from frozendict import frozendict

from hgraph import graph, TS, TSD, TSS, TSL, SIZE, map_, reduce, HgTypeMetaData, SCALAR, Size, REF
from hgraph._runtime._map import _build_map_wiring_node_and_inputs
from hgraph._wiring._map_wiring_node import TsdMapWiringSignature, TslMapWiringSignature
from hgraph.nodes import add_, debug_print, const
from hgraph.test import eval_node


@graph
def f_sum(key: TS[SCALAR], lhs: TS[int], rhs: TS[int]) -> TS[int]:
    a = add_(lhs, rhs)
    debug_print("key", key)
    debug_print("sum", a)
    return a


def test_guess_arguments_f_sum_lhs():
    lhs = const(frozendict({'a': 1}), TSD[str, TS[int]])
    rhs = const(2)
    wiring_node, wiring_inputs = _build_map_wiring_node_and_inputs(f_sum, f_sum.signature, lhs, rhs)
    signature: TsdMapWiringSignature = wiring_node.signature
    assert signature.args == ('lhs', 'rhs', '__keys__')
    assert signature.key_tp == HgTypeMetaData.parse(str)
    assert signature.key_arg == 'key'
    assert signature.output_type == HgTypeMetaData.parse(TSD[str, REF[TS[int]]])
    assert signature.input_types == frozendict(
        {'lhs': HgTypeMetaData.parse(TSD[str, REF[TS[int]]]), 'rhs': HgTypeMetaData.parse(REF[TS[int]]),
         '__keys__': HgTypeMetaData.parse(TSS[str])})
    assert signature.multiplexed_args == frozenset({'lhs', })
    assert wiring_inputs.keys() == {'lhs', 'rhs', '__keys__'}


def test_guess_arguments_f_sum_keys():
    lhs = const(frozendict({'a': 1}), TSD[str, TS[int]])
    rhs = const(2)
    keys = const(frozenset({'a', 'b'}), TSS[str])
    wiring_node, wiring_inputs = _build_map_wiring_node_and_inputs(f_sum, f_sum.signature, lhs, rhs, __keys__=keys)
    signature: TsdMapWiringSignature = wiring_node.signature
    assert signature.args == ('lhs', 'rhs', '__keys__')
    assert signature.key_tp == HgTypeMetaData.parse(str)
    assert signature.key_arg == 'key'
    assert signature.output_type == HgTypeMetaData.parse(TSD[str, REF[TS[int]]])
    assert signature.input_types == frozendict(
        {'lhs': HgTypeMetaData.parse(TSD[str, REF[TS[int]]]), 'rhs': HgTypeMetaData.parse(REF[TS[int]]),
         '__keys__': HgTypeMetaData.parse(TSS[str])})
    assert signature.multiplexed_args == frozenset({'lhs', })
    assert wiring_inputs.keys() == {'lhs', 'rhs', '__keys__'}


def test_guess_arguments_add_keys():
    lhs = const(frozendict({'a': 1}), TSD[str, TS[int]])
    rhs = const(2)
    keys = const(frozenset({'a', 'b'}), TSS[str])
    wiring_node, wiring_inputs = _build_map_wiring_node_and_inputs(add_, add_.signature, lhs, rhs, __keys__=keys)
    signature: TsdMapWiringSignature = wiring_node.signature
    assert signature.args == ('lhs', 'rhs', '__keys__')
    assert signature.key_tp == HgTypeMetaData.parse(str)
    assert signature.key_arg == None
    assert signature.output_type == HgTypeMetaData.parse(TSD[str, REF[TS[int]]])
    assert signature.input_types == frozendict(
        {'lhs': HgTypeMetaData.parse(TSD[str, REF[TS[int]]]), 'rhs': HgTypeMetaData.parse(REF[TS[int]]),
         '__keys__': HgTypeMetaData.parse(TSS[str])})
    assert signature.multiplexed_args == frozenset({'lhs', })
    assert wiring_inputs.keys() == {'lhs', 'rhs', '__keys__'}


def test_guess_arguments_add_no_keys():
    lhs = const(frozendict({'a': 1}), TSD[str, TS[int]])
    rhs = const(2)
    keys = const(frozenset({'a', 'b'}), TSS[str])
    wiring_node, wiring_inputs = _build_map_wiring_node_and_inputs(add_, add_.signature, lhs, rhs)
    signature: TsdMapWiringSignature = wiring_node.signature
    assert signature.args == ('lhs', 'rhs', '__keys__')
    assert signature.key_tp == HgTypeMetaData.parse(str)
    assert signature.key_arg == None
    assert signature.output_type == HgTypeMetaData.parse(TSD[str, REF[TS[int]]])
    assert signature.input_types == frozendict(
        {'lhs': HgTypeMetaData.parse(TSD[str, REF[TS[int]]]), 'rhs': HgTypeMetaData.parse(REF[TS[int]]),
         '__keys__': HgTypeMetaData.parse(TSS[str])})
    assert signature.multiplexed_args == frozenset({'lhs', })
    assert wiring_inputs.keys() == {'lhs', 'rhs', '__keys__'}


def test_guess_arguments_f_sum_lhs_tsl():
    lhs = const(tuple([1, 1]), TSL[TS[int], Size[2]])
    rhs = const(2)
    wiring_node, wiring_inputs = _build_map_wiring_node_and_inputs(f_sum, f_sum.signature, lhs, rhs, __key_arg__='key')
    signature: TslMapWiringSignature = wiring_node.signature
    assert signature.args == ('lhs', 'rhs')
    assert signature.size_tp == HgTypeMetaData.parse(Size[2])
    assert signature.key_arg == 'key'
    assert signature.output_type == HgTypeMetaData.parse(TSL[REF[TS[int]], Size[2]])
    assert signature.input_types == frozendict(
        {'lhs': HgTypeMetaData.parse(TSL[REF[TS[int]], Size[2]]), 'rhs': HgTypeMetaData.parse(REF[TS[int]])})
    assert signature.multiplexed_args == frozenset({'lhs', })
    assert wiring_inputs.keys() == {'lhs', 'rhs'}


def test_guess_arguments_f_sum_keys_tsl():
    lhs = const(tuple([1, 1]), TSL[TS[int], Size[2]])
    rhs = const(2)
    keys = const(tuple([True, True]), TSL[TS[bool], Size[2]])
    wiring_node, wiring_inputs = _build_map_wiring_node_and_inputs(f_sum, f_sum.signature, lhs, rhs, __index__=keys,
                                                                   __key_arg__='key')
    signature: TslMapWiringSignature = wiring_node.signature
    assert signature.args == ('lhs', 'rhs', '__index__')
    assert signature.key_arg == 'key'
    assert signature.output_type == HgTypeMetaData.parse(TSL[REF[TS[int]], Size[2]])
    assert signature.input_types == frozendict(
        {'lhs': HgTypeMetaData.parse(TSL[REF[TS[int]], Size[2]]), 'rhs': HgTypeMetaData.parse(REF[TS[int]]),
         '__index__': HgTypeMetaData.parse(TSL[TS[bool], Size[2]])})
    assert signature.multiplexed_args == frozenset({'lhs', })
    assert wiring_inputs.keys() == {'lhs', 'rhs', '__index__'}


def test_guess_arguments_add_keys_tsl():
    lhs = const(tuple([1, 1]), TSL[TS[int], Size[2]])
    rhs = const(2)
    keys = const(tuple([True, True]), TSL[TS[bool], Size[2]])
    wiring_node, wiring_inputs = _build_map_wiring_node_and_inputs(add_, add_.signature, lhs, rhs, __index__=keys)
    signature: TsdMapWiringSignature = wiring_node.signature
    assert signature.args == ('lhs', 'rhs', '__index__')
    assert signature.key_arg == None
    assert signature.output_type == HgTypeMetaData.parse(TSL[REF[TS[int]], Size[2]])
    assert signature.input_types == frozendict(
        {'lhs': HgTypeMetaData.parse(TSL[REF[TS[int]], Size[2]]), 'rhs': HgTypeMetaData.parse(REF[TS[int]]),
         '__index__': HgTypeMetaData.parse(TSL[TS[bool], Size[2]])})
    assert signature.multiplexed_args == frozenset({'lhs', })
    assert wiring_inputs.keys() == {'lhs', 'rhs', '__index__'}


def test_guess_arguments_add_no_keys_tsl():
    lhs = const(tuple([1, 1]), TSL[TS[int], Size[2]])
    rhs = const(2)
    keys = const(frozenset({'a', 'b'}), TSS[str])
    wiring_node, wiring_inputs = _build_map_wiring_node_and_inputs(add_, add_.signature, lhs, rhs)
    signature: TsdMapWiringSignature = wiring_node.signature
    assert signature.args == ('lhs', 'rhs')
    assert signature.output_type == HgTypeMetaData.parse(TSL[REF[TS[int]], Size[2]])
    assert signature.input_types == frozendict(
        {'lhs': HgTypeMetaData.parse(TSL[REF[TS[int]], Size[2]]), 'rhs': HgTypeMetaData.parse(REF[TS[int]])})
    assert signature.multiplexed_args == frozenset({'lhs', })
    assert wiring_inputs.keys() == {'lhs', 'rhs'}


def test_tsd_map_wiring():
    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = map_(f_sum, lhs=ts1, rhs=ts2)
        return m

    _test_tsd_map(map_test)


def test_tsd_map_wiring_no_key():
    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = map_(add_, lhs=ts1, rhs=ts2)
        return m

    _test_tsd_map(map_test)


def test_tsd_map_wiring_no_key_no_kwargs():
    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = map_(add_, ts1, ts2, keys=keys)
        return m

    _test_tsd_map(map_test)


def test_tsd_map_wiring_no_kwargs():
    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = map_(add_, ts1, ts2, keys=keys)
        return m

    _test_tsd_map(map_test)


def _test_tsd_map(map_test):
    out = eval_node(map_test, [{'a', 'b'}, ], [{'a': 1}, {'b': 2}], [{'a': 2}, {'b': 3}])
    assert out == [{'a': 3}, {'b': 5}]


# @pytest.mark.xfail(reason="Not implemented", strict=True)
# def test_tsl_map_wiring():
#     @graph
#     def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
#         m = map_(f_sum, lhs=ts1, rhs=ts2)
#         return m
#
#     _test_tsl_map(map_test)


# @pytest.mark.xfail(reason="Not implemented", strict=True)
# def test_tsl_map_wiring_no_key():
#     @graph
#     def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
#         m = map_(add_, lhs=ts1, rhs=ts2)
#         return m
#
#     _test_tsl_map(map_test)


@pytest.mark.xfail(reason="Not implemented", strict=True)
def test_tsl_map_wiring_no_key_no_kwargs():
    @graph
    def map_test(index: TSL[TS[bool], SIZE], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = map_(add_, ts1, ts2, index=index)
        return m

    _test_tsl_map(map_test)


@pytest.mark.xfail(reason="Not implemented", strict=True)
def test_tsl_map_wiring_no_kwargs():
    @graph
    def map_test(index: TSL[TS[bool], SIZE], ts1: TSL[TS[int], SIZE], ts2: TSL[TS[int], SIZE]) -> TSL[TS[int], SIZE]:
        m = map_(add_, ts1, ts2, index=index)
        return m

    _test_tsl_map(map_test)


def _test_tsl_map(map_test):
    out = eval_node(map_test, [{0: True, 1: True}, ], [{0: 1}, {1: 2}], [{0: 2}, {1: 3}])
    assert out == [{0: 3}, {1: 5}]


@pytest.mark.xfail(reason="Not implemented", strict=True)
def test_tsd_reduce():
    @graph
    def reduce_test(tsd: TSD[str, TS[int]]) -> TS[int]:
        return reduce(add_, tsd, 0)

    assert eval_node(reduce_test, [None, {'a': 1}, {'b': 2}]) == [0, 1, 3]


@pytest.mark.xfail(reason="Not implemented", strict=True)
def test_tsl_reduce():
    @graph
    def reduce_test(tsl: TSL[TS[int], SIZE]) -> TS[int]:
        return reduce(add_, tsl, 0)

    assert eval_node(reduce_test, [None, {0: 1}, {1: 2}]) == [0, 1, 3]
