from hgraph._operators._stream import filter_
import pytest
from frozendict import frozendict
import pytest
pytestmark = pytest.mark.smoke

from hgraph import (
    graph,
    TS,
    TSD,
    TSS,
    TSL,
    SIZE,
    map_,
    pass_through,
    pass_through,
    reduce,
    HgTypeMetaData,
    SCALAR,
    Size,
    REF,
    REMOVE_IF_EXISTS,
    REMOVE,
    compute_node,
    SCHEDULER,
    CustomMessageWiringError,
    WiringGraphContext,
    add_,
    format_,
    const,
    debug_print,
    switch_,
    nothing,
    Removed,
    sum_,
    valid, if_, if_then_else,
)
from hgraph._wiring._map import _build_map_wiring
from hgraph._wiring._wiring_node_class._map_wiring_node import TsdMapWiringSignature, TslMapWiringSignature
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext
from hgraph._operators import pass_through_node
from hgraph.test import eval_node


@graph
def f_sum(key: TS[SCALAR], lhs: TS[int], rhs: TS[int]) -> TS[int]:
    a = add_(lhs, rhs)
    debug_print("key", key)
    debug_print("sum", a)
    return a


def test_guess_arguments_f_sum_lhs():
    with WiringNodeInstanceContext(), WiringGraphContext(None):
        lhs = const(frozendict({"a": 1}), TSD[str, TS[int]])
        rhs = const(2)
        wiring_node, wiring_inputs, _ = _build_map_wiring(f_sum, f_sum.signature, lhs, rhs)
        signature: TsdMapWiringSignature = wiring_node.signature
        assert signature.args == ("lhs", "rhs", "__keys__")
        assert signature.key_tp == HgTypeMetaData.parse_type(str)
        assert signature.key_arg == "key"
        assert signature.output_type == HgTypeMetaData.parse_type(TSD[str, REF[TS[int]]])
        assert signature.input_types == frozendict({
            "lhs": HgTypeMetaData.parse_type(TSD[str, REF[TS[int]]]),
            "rhs": HgTypeMetaData.parse_type(REF[TS[int]]),
            "__keys__": HgTypeMetaData.parse_type(TSS[str]),
        })
        assert signature.multiplexed_args == frozenset({
            "lhs",
        })
        assert wiring_inputs.keys() == {"lhs", "rhs", "__keys__"}


def test_guess_arguments_f_sum_keys():
    with WiringNodeInstanceContext(), WiringGraphContext(None):
        lhs = const(frozendict({"a": 1}), TSD[str, TS[int]])
        rhs = const(2)
        keys = const(frozenset({"a", "b"}), TSS[str])
        wiring_node, wiring_inputs, _ = _build_map_wiring(f_sum, f_sum.signature, lhs, rhs, __keys__=keys)
        signature: TsdMapWiringSignature = wiring_node.signature
        assert signature.args == ("lhs", "rhs", "__keys__")
        assert signature.key_tp == HgTypeMetaData.parse_type(str)
        assert signature.key_arg == "key"
        assert signature.output_type == HgTypeMetaData.parse_type(TSD[str, REF[TS[int]]])
        assert signature.input_types == frozendict({
            "lhs": HgTypeMetaData.parse_type(TSD[str, REF[TS[int]]]),
            "rhs": HgTypeMetaData.parse_type(REF[TS[int]]),
            "__keys__": HgTypeMetaData.parse_type(TSS[str]),
        })
        assert signature.multiplexed_args == frozenset({
            "lhs",
        })
        assert wiring_inputs.keys() == {"lhs", "rhs", "__keys__"}


def test_guess_arguments_add_keys():
    from hgraph._impl._operators._scalar_operators import add_scalars

    with WiringNodeInstanceContext(), WiringGraphContext(None):
        lhs = const(frozendict({"a": 1}), TSD[str, TS[int]])
        rhs = const(2)
        keys = const(frozenset({"a", "b"}), TSS[str])
        wiring_node, wiring_inputs, _ = _build_map_wiring(add_scalars, add_scalars.signature, lhs, rhs, __keys__=keys)
        signature: TsdMapWiringSignature = wiring_node.signature
        assert signature.args == ("lhs", "rhs", "__keys__")
        assert signature.key_tp == HgTypeMetaData.parse_type(str)
        assert signature.key_arg == None
        assert signature.output_type == HgTypeMetaData.parse_type(TSD[str, REF[TS[int]]])
        assert signature.input_types == frozendict({
            "lhs": HgTypeMetaData.parse_type(TSD[str, REF[TS[int]]]),
            "rhs": HgTypeMetaData.parse_type(REF[TS[int]]),
            "__keys__": HgTypeMetaData.parse_type(TSS[str]),
        })
        assert signature.multiplexed_args == frozenset({
            "lhs",
        })
        assert wiring_inputs.keys() == {"lhs", "rhs", "__keys__"}


def test_guess_arguments_add_no_keys():
    from hgraph._impl._operators._scalar_operators import add_scalars

    with WiringNodeInstanceContext(), WiringGraphContext(None):
        lhs = const(frozendict({"a": 1}), TSD[str, TS[int]])
        rhs = const(2)
        wiring_node, wiring_inputs, _ = _build_map_wiring(add_scalars, add_scalars.signature, lhs, rhs)
        signature: TsdMapWiringSignature = wiring_node.signature
        assert signature.args == ("lhs", "rhs", "__keys__")
        assert signature.key_tp == HgTypeMetaData.parse_type(str)
        assert signature.key_arg == None
        assert signature.output_type == HgTypeMetaData.parse_type(TSD[str, REF[TS[int]]])
        assert signature.input_types == frozendict({
            "lhs": HgTypeMetaData.parse_type(TSD[str, REF[TS[int]]]),
            "rhs": HgTypeMetaData.parse_type(REF[TS[int]]),
            "__keys__": HgTypeMetaData.parse_type(TSS[str]),
        })
        assert signature.multiplexed_args == frozenset({
            "lhs",
        })
        assert wiring_inputs.keys() == {"lhs", "rhs", "__keys__"}


def test_guess_arguments_f_sum_lhs_tsl():
    with WiringNodeInstanceContext(), WiringGraphContext(None):
        lhs = const(tuple([1, 1]), TSL[TS[int], Size[2]])
        rhs = const(2)
        wiring_node, wiring_inputs, _ = _build_map_wiring(f_sum, f_sum.signature, lhs, rhs, __key_arg__="key")
        signature: TslMapWiringSignature = wiring_node.signature
        assert signature.args == ("lhs", "rhs")
        assert signature.size_tp == HgTypeMetaData.parse_type(Size[2])
        assert signature.key_arg == "key"
        assert signature.output_type == HgTypeMetaData.parse_type(TSL[REF[TS[int]], Size[2]])
        assert signature.input_types == frozendict({
            "lhs": HgTypeMetaData.parse_type(TSL[REF[TS[int]], Size[2]]),
            "rhs": HgTypeMetaData.parse_type(REF[TS[int]]),
        })
        assert signature.multiplexed_args == frozenset({
            "lhs",
        })
        assert wiring_inputs.keys() == {"lhs", "rhs"}


def test_guess_arguments_add_no_keys_tsl():
    from hgraph._impl._operators._scalar_operators import add_scalars

    with WiringNodeInstanceContext(), WiringGraphContext(None):
        lhs = const(tuple([1, 1]), TSL[TS[int], Size[2]])
        rhs = const(2)
        wiring_node, wiring_inputs, _ = _build_map_wiring(add_scalars, add_scalars.signature, lhs, rhs)
        signature: TsdMapWiringSignature = wiring_node.signature
        assert signature.args == ("lhs", "rhs")
        assert signature.output_type == HgTypeMetaData.parse_type(TSL[REF[TS[int]], Size[2]])
        assert signature.input_types == frozendict({
            "lhs": HgTypeMetaData.parse_type(TSL[REF[TS[int]], Size[2]]),
            "rhs": HgTypeMetaData.parse_type(REF[TS[int]]),
        })
        assert signature.multiplexed_args == frozenset({
            "lhs",
        })
        assert wiring_inputs.keys() == {"lhs", "rhs"}


@pytest.mark.smoke
def test_tsd_map_wiring():
    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = map_(f_sum, lhs=ts1, rhs=ts2)
        return m

    _test_tsd_map(map_test)


def test_tsd_map_wiring_no_key():
    from hgraph._impl._operators._scalar_operators import add_scalars

    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = map_(add_scalars, lhs=ts1, rhs=ts2)
        return m

    _test_tsd_map(map_test)


def test_tsd_map_wiring_no_key_no_kwargs():
    from hgraph._impl._operators._scalar_operators import add_scalars

    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = map_(add_scalars, ts1, ts2, __keys__=keys)
        return m

    _test_tsd_map(map_test)


def test_tsd_map_wiring_no_kwargs():
    from hgraph._impl._operators._scalar_operators import add_scalars

    @graph
    def map_test(keys: TSS[str], ts1: TSD[str, TS[int]], ts2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        m = map_(add_scalars, ts1, ts2, __keys__=keys)
        return m

    _test_tsd_map(map_test)


def _test_tsd_map(map_test):
    out = eval_node(
        map_test,
        [
            {"a", "b"},
        ],
        [{"a": 1}, {"b": 2}],
        [{"a": 2}, {"b": 3}],
    )
    assert out == [{"a": 3}, {"b": 5}]


@pytest.mark.smoke
def test_tsl_map_wiring():
    @graph
    def map_test(index: TSL[TS[bool], SIZE], ts1: TSL[TS[int], SIZE], ts2: TSL[TS[int], SIZE]) -> TSL[TS[int], SIZE]:
        m = map_(f_sum, lhs=ts1, rhs=ts2)
        return m

    _test_tsl_map(map_test)


def test_tsl_map_wiring_no_key():
    from hgraph._impl._operators._scalar_operators import add_scalars

    @graph
    def map_test(index: TSL[TS[bool], SIZE], ts1: TSL[TS[int], SIZE], ts2: TSL[TS[int], SIZE]) -> TSL[TS[int], SIZE]:
        m = map_(add_scalars, lhs=ts1, rhs=ts2)
        return m

    _test_tsl_map(map_test)


def _test_tsl_map(map_test):
    out = eval_node(
        map_test,
        [
            {0: 1, 1: 0},
        ],
        [{0: 1}, {1: 2}],
        [{0: 2}, {1: 3}],
        resolution_dict={"index": TSL[TS[bool], Size[2]], "ts1": TSL[TS[int], Size[2]], "ts2": TSL[TS[int], Size[2]]},
    )
    assert out == [{0: 3}, {1: 5}]


@pytest.mark.parametrize(
    ["inputs", "size", "expected"],
    [
        [[None, {0: 1}, None, {1: 2}], Size[2], [0, 1, None, 3]],
        [[None, {0: 1, 3: 4}, {1: 2, 2: 3}], Size[4], [0, 5, 10]],
        [[None, {0: 1, 3: 4}, {1: 2, 2: 3}, {4: 8}], Size[5], [0, 5, 10, 18]],
        [[None, {0: 1, 3: 4}, {1: 2, 2: 3}, {4: 8, 5: 9}], Size[6], [0, 5, 10, 27]],
    ],
)
def test_tsl_reduce_lambda(inputs, size, expected):
    @graph
    def reduce_test(tsl: TSL[TS[int], SIZE]) -> TS[int]:
        return reduce(lambda x, y: x + y, tsl, 0)

    assert eval_node(reduce_test, inputs, resolution_dict={"tsl": TSL[TS[int], size]}) == expected


@pytest.mark.parametrize(
    ["inputs", "expected"],
    [
        [[{0: 1, 1: 2}, {1: REMOVE_IF_EXISTS}, {1: 3}], [{0: 1, 1: 2}, {1: REMOVE}, {1: 3}]],
        [
            [{0: 1}, {1: 2}, {0: REMOVE_IF_EXISTS, 1: REMOVE_IF_EXISTS}, {2: 3}],
            [{0: 1}, {1: 2}, {0: REMOVE, 1: REMOVE}, {2: 3}],
        ],
    ],
)
def test_tsd_map_life_cycle(inputs, expected):
    @graph
    def map_graph(tsd: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(pass_through_node, tsd)

    out = eval_node(map_graph, inputs)
    assert out == expected


def test_map_over_compute_node_with_injectables():
    @compute_node
    def cn_with_scheduler(ts: TS[int], _scheduler: SCHEDULER = None) -> TS[int]:
        return ts.value

    @graph
    def map_cn(tsd: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(cn_with_scheduler, tsd)

    assert eval_node(map_cn, [{1: 1, 2: 2}]) == [{1: 1, 2: 2}]


def test_map_over_lambda():
    @graph
    def map_l(tsd: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(lambda key, v: v + 1, tsd)

    assert eval_node(map_l, [{1: 1, 2: 2}]) == [{1: 2, 2: 3}]


def test_map_over_lambda_no_key():
    @graph
    def map_l(tsd: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return map_(lambda v: v + 1, tsd)

    assert eval_node(map_l, [{1: 1, 2: 2}]) == [{1: 2, 2: 3}]


def test_map_over_lambda_passthru():
    @graph
    def map_l(tsd: TSD[int, TS[int]], a: TS[int]) -> TSD[int, TS[int]]:
        return map_(lambda v, u: v + u, tsd, a)

    assert eval_node(map_l, [{1: 1, 2: 2}], [2]) == [{1: 3, 2: 4}]


def test_map_over_lambda_tsl_passthru():
    @graph
    def map_l(tsd: TSD[int, TS[int]], a: TSL[TS[int], Size[2]]) -> TSD[int, TS[int]]:
        return map_(lambda v, u: v + u[0] + u[1], tsd, a)

    assert eval_node(map_l, [{1: 1, 2: 2}], [(1, 2)]) == [{1: 4, 2: 5}]


def test_map_over_lambda_errors():
    @graph
    def map_l(tsd: TSD[int, TS[int]], a: TS[int]) -> TSD[int, TS[int]]:
        return map_(lambda v, u, t: v + u, tsd, a)

    with pytest.raises(CustomMessageWiringError, match="no input"):
        eval_node(map_l, [{1: 1, 2: 2}], [2])


def test_map_over_lambda_errors_2():
    @graph
    def map_l(tsd: TSD[int, TS[int]], a: TS[int]) -> TSD[int, TS[int]]:
        return map_(lambda v, u: v + u, tsd, a, a)

    with pytest.raises(CustomMessageWiringError, match="not used"):
        eval_node(map_l, [{1: 1, 2: 2}], [2])


def test_map_over_lambda_args():
    @graph
    def map_l(tsd: TSD[int, TS[int]], a: TS[int], b: TS[int]) -> TSD[int, TS[int]]:
        return map_(lambda *args: sum_(args), tsd, a, b)

    assert eval_node(map_l, [{1: 1, 2: 2}], [2], [3]) == [{1: 6, 2: 7}]


def test_map_restricted_keys():
    @graph
    def g(keys: TSS[str], value: TSD[str, TS[int]]) -> TSD[str, TS[str]]:
        return map_(lambda key, v: format_("{}_{}", key, v), value, __keys__=keys)

    assert eval_node(
        g,
        [
            frozenset({"a", "b"}),
        ],
        [{"a": 1, "b": 2}, {"c": 3}],
    ) == [frozendict({"a": "a_1", "b": "b_2"}), None]


def test_map_re_added_keys():
    @graph
    def g(keys: TSS[str], value: TSD[str, TS[int]]) -> TSD[str, TS[str]]:
        return map_(lambda key, v: format_("{}_{}", key, v), value, __keys__=keys)

    assert eval_node(
        g,
        [
            {"a", "b"},
            {Removed("a")},
            {"a"},
        ],
        [{"a": 1, "b": 2}, {"c": 3}],
    ) == [{"a": "a_1", "b": "b_2"}, {"a": REMOVE}, {"a": "a_1"}]


def test_map_re_added_references():
    @graph
    def g(keys: TSS[str], value: TSD[str, TS[int]]) -> TSD[str, TS[str]]:
        value = map_(lambda v: v, value)
        return map_(lambda key, v: format_("{}_{}", key, v), value, __keys__=keys)

    assert eval_node(
        g,
        [
            {"a", "b"},
            {Removed("a")},
            {"a"},
        ],
        [{"a": 1, "b": 2}, {"c": 3}],
    ) == [{"a": "a_1", "b": "b_2"}, {"a": REMOVE}, {"a": "a_1"}]


def test_map_preexisting_keys():
    @graph
    def g(keys: TSS[str], flag: TS[bool]) -> TSD[str, TS[str]]:
        return switch_(
            flag, {False: lambda k: nothing(TSD[str, TS[str]]), True: lambda k: map_(lambda key: key, __keys__=k)}, keys
        )

    assert eval_node(g, [{"a", "b"}, None, {"c"}], [False, True, None]) == [
        None,
        {"a": "a", "b": "b"},
        {"c": "c"},
    ]


def test_map_reference_cleanup():
    @graph
    def g(value: TSD[str, TS[int]]) -> TSD[str, TS[str]]:
        m1 = map_(lambda key, v: format_("{}_{}_1", key, v), value)
        m2 = map_(lambda key, v: format_("{}_2", v), m1)
        return m2

    assert eval_node(
        g,
        [{"a": 1, "b": 2}, {"b": REMOVE}, {"a": 2}],
    ) == [{"a": "a_1_1_2", "b": "b_2_1_2"}, {"b": REMOVE}, {"a": "a_2_1_2"}]
    
    
def test_map_reference_cleanup_2():
    @graph
    def g(value: TSD[str, TS[int]], selection: TSS[str]) -> TSD[str, TS[bool]]:
        m1 = map_(lambda key, v: format_("{}_1", v), value)
        return map_(lambda key, m: valid(m[key]), pass_through(m1), __keys__=selection)

    assert eval_node(
        g,
        [{"a": 1, "b": 2}, {"b": REMOVE}, {"a": 2}],
        [{"b"}, None, None],
    ) == [{"b": True}, {"b": False}, None]


def test_map_input_rebind_to_nonpeer():
    @graph
    def g(ts: TS[int]) -> TSD[str, TS[int]]:
        initial = const({'a': 1, 'b': 2}, TSD[str, TS[int]])
        source = switch_(ts > 0, {
            # when ts=0 just pass the const through so that output map gets a peer
            False: lambda i, replace_refs: i,
            # on ts=1 push const through a map to get a non-peer TSD
            # then on ts=2 replace the references with the x+1 nodes (i.e. new refs will tick out of the map), and values will jump by 1
            True: lambda i, replace_refs: map_(lambda x, replace_refs_: if_then_else(replace_refs_, x+1, x), i, replace_refs),
        }, initial, ts > 1)

        return map_(lambda x, y: x + y, source, ts)  # note ts is added to all values every tick

    assert eval_node(g, [0, 1, 2, 3]) == [
        {'a': 1, 'b': 2},
        {'a': 2, 'b': 3},
        {'a': 4, 'b': 5},
        {'a': 5, 'b': 6},
    ]


def test_map_output_invalid():
    @graph
    def delayed_output(key: TS[int], ts: TS[str]) -> TS[str]:
        return switch_(key > 0, {
            False: lambda i: nothing(TS[str]),
            True: lambda i: i,
        }, ts)
    
    @graph
    def g(ts: TSD[int, TS[str]], snap: TS[bool]) -> TSD[int, TS[str]]:
        return filter_(snap, map_(delayed_output, ts))
    
    assert eval_node(g, [{1: '1', 0: '0'}], [False, True]) == [
        None,
        {1: '1'},
    ]
    
