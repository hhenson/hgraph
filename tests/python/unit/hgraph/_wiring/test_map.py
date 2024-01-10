import pytest
from frozendict import frozendict

from hgraph import graph, TS, TSD, TSS, TSL, SIZE, map_, reduce, HgTypeMetaData, SCALAR, Size, REF, REMOVE_IF_EXISTS, \
    REMOVE
from hgraph._wiring._map import _build_map_wiring_node_and_inputs
from hgraph._wiring._map_wiring_node import TsdMapWiringSignature, TslMapWiringSignature
from hgraph.nodes import add_, debug_print, const, pass_through
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


def test_guess_arguments_add_no_keys_tsl():
    lhs = const(tuple([1, 1]), TSL[TS[int], Size[2]])
    rhs = const(2)
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


def test_tsl_map_wiring():
    @graph
    def map_test(index: TSL[TS[bool], SIZE], ts1: TSL[TS[int], SIZE], ts2: TSL[TS[int], SIZE]) -> TSL[TS[int], SIZE]:
        m = map_(f_sum, lhs=ts1, rhs=ts2)
        return m

    _test_tsl_map(map_test)


def test_tsl_map_wiring_no_key():
    @graph
    def map_test(index: TSL[TS[bool], SIZE], ts1: TSL[TS[int], SIZE], ts2: TSL[TS[int], SIZE]) -> TSL[TS[int], SIZE]:
        m = map_(add_, lhs=ts1, rhs=ts2)
        return m

    _test_tsl_map(map_test)


def _test_tsl_map(map_test):
    out = eval_node(map_test, [{0: 1, 1: 0}, ], [{0: 1}, {1: 2}], [{0: 2}, {1: 3}],
                    resolution_dict={'index': TSL[TS[bool], Size[2]],
                                     'ts1': TSL[TS[int], Size[2]],
                                     'ts2': TSL[TS[int], Size[2]]})
    assert out == [{0: 3}, {1: 5}]


@pytest.mark.parametrize(
    ["inputs", "expected"],
    [
        [[None, {'a': 1}, {'a': REMOVE_IF_EXISTS}], [0, 1, 0]],
        [[None, {'a': 1}, {'b': 2}, {'b': REMOVE_IF_EXISTS}, {'a': REMOVE_IF_EXISTS}], [0, 1, 3, 1, 0]],
        [[{'a': 1, 'b': 2, 'c': 3}, {'b': REMOVE_IF_EXISTS}, {'a': REMOVE_IF_EXISTS}], [6, 4, 3]],
        [[{'a': 1}, {'b': 2}, {'c': 3}, {'d': 4}, {'e': 5}], [1, 3, 6, 10, 15]],

        [[{(chr(ord('a') + i)): i for i in range(26)},
          {(chr(ord('a') + i)): REMOVE_IF_EXISTS for i in range(26)}, ], [325, 0]],

        [
            [
                {(chr(ord('a') + i)): i for i in range(26)},
                {(chr(ord('a') + i)): REMOVE_IF_EXISTS for i in range(20)},
                {(chr(ord('a') + i)): i for i in range(20)},
                {(chr(ord('a') + i)): REMOVE_IF_EXISTS for i in range(26)},
                {(chr(ord('a') + i)): i for i in range(26)}
            ], [325, 135, 325, 0, 325]]
    ]
)
def test_tsd_reduce(inputs, expected):
    @graph
    def reduce_test(tsd: TSD[str, TS[int]]) -> TS[int]:
        return reduce(add_, tsd, 0)

    assert eval_node(reduce_test, inputs) == expected


# assert eval_node(reduce_test, inputs, __trace__=True) == expected


@pytest.mark.parametrize(
    ["inputs", 'size', "expected"],
    [
        [[None, {0: 1}, None, {1: 2}], Size[2], [0, 1, None, 3]],
        [[None, {0: 1, 3: 4}, {1: 2, 2: 3}], Size[4], [0, 5, 10]],
        [[None, {0: 1, 3: 4}, {1: 2, 2: 3}, {4: 8}], Size[5], [0, 5, 10, 18]],
        [[None, {0: 1, 3: 4}, {1: 2, 2: 3}, {4: 8, 5: 9}], Size[6], [0, 5, 10, 27]],
    ]
)
def test_tsl_reduce(inputs, size, expected):
    @graph
    def reduce_test(tsl: TSL[TS[int], SIZE]) -> TS[int]:
        return reduce(add_, tsl, 0)

    assert eval_node(reduce_test, inputs, resolution_dict={'tsl': TSL[TS[int], size]}) == expected


@pytest.mark.parametrize(
    ["inputs", "expected"],
    [
        [[{0: 1, 1: 2}, {1: REMOVE_IF_EXISTS}, {1: 3}],
         [{0: 1, 1: 2}, {1: REMOVE}, {1: 3}]],
        [[{0: 1}, {1: 2}, {0: REMOVE_IF_EXISTS, 1: REMOVE_IF_EXISTS}, {2: 3}],
         [{0: 1}, {1: 2}, {0: REMOVE, 1: REMOVE}, {2: 3}]]
    ]
)
def test_tsd_map_life_cycle(inputs, expected):
    @graph
    def map_graph(tsd: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(pass_through, tsd)

    out = eval_node(map_graph, inputs)
    assert out == expected
