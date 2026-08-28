# Ported from release/0.5:hgraph_unit_tests/_wiring/test_switch.py at
# 4760fccadd5368b0482393e5acb0ceaac48518e9
from frozendict import frozendict

from hgraph import (
    DEFAULT,
    EvaluationClock,
    MIN_TD,
    REMOVE,
    Removed,
    SCALAR,
    TS,
    TSB,
    TSD,
    TSS,
    TimeSeriesSchema,
    add_,
    combine,
    compute_node,
    const,
    dedup,
    default,
    generator,
    graph,
    lag,
    map_,
    print_,
    reduce,
    switch_,
)
from hgraph.test import eval_node

import pytest

pytestmark = pytest.mark.smoke


def test_switch():
    @graph
    def _add(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return lhs + rhs

    @graph
    def _sub(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return lhs - rhs

    @graph
    def switch_test(key: TS[str], lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return switch_(key, {"add": _add, "sub": _sub}, lhs, rhs)

    assert eval_node(switch_test, ["add", "sub"], [1, 2], [3, 4]) == [4, -2]


def test_switch_fixed_tuple_keys_use_the_key_port_schema():
    @graph
    def switch_test(key: TS[tuple[bool, bool]], value: TS[int]) -> TS[int]:
        return switch_(
            key,
            {
                (True, True): lambda ts: ts + 1,
                (False, False): lambda ts: ts - 1,
            },
            value,
        )

    assert eval_node(
        switch_test,
        [(True, True), (False, False)],
        [10, 20],
    ) == [11, 19]


def test_switch_with_graph():
    @graph
    def graph_1(value: SCALAR) -> TS[SCALAR]:
        return const(f"{value}_1")

    @graph
    def graph_2(value: SCALAR) -> TS[SCALAR]:
        return const(f"{value}_2")

    @graph
    def switch_test(key: TS[str], value: SCALAR) -> TS[SCALAR]:
        return switch_(key, {"one": graph_1, "two": graph_2}, value)

    assert eval_node(switch_test, ["one", "two"], "test") == ["test_1", "test_2"]


STARTED = 0
STOPPED = 0


def test_stop_start():
    global STARTED, STOPPED
    STARTED = 0
    STOPPED = 0

    @compute_node
    def g(key: TS[str]) -> TS[str]:
        return key.value

    @g.start
    def g_start():
        global STARTED
        STARTED += 1

    @g.stop
    def g_stop():
        global STOPPED
        STOPPED += 1

    @graph
    def switch_test(key: TS[str]) -> TS[str]:
        return switch_(key, {"one": g, "two": g})

    assert eval_node(switch_test, ["one", "two"]) == ["one", "two"]
    assert STARTED == 2
    assert STOPPED == 2


@generator
def _generator(key: str, _clock: EvaluationClock = None) -> TS[str]:
    for i in range(5):
        yield _clock.next_cycle_evaluation_time, f"{key}_{i}"


@graph
def one_() -> TS[str]:
    return _generator("one")


@graph
def two_() -> TS[str]:
    return _generator("two")


@graph
def _switch(key: TS[str]) -> TS[str]:
    key = default(const("two", delay=MIN_TD * 3), key)
    return switch_(key, {"one": one_, "two": two_})


@graph
def _map(keys: TSS[str]) -> TSD[str, TS[str]]:
    return map_(_switch, __keys__=keys, __key_arg__="key")


def test_nested_switch():
    fd = frozendict
    assert eval_node(_map, [{"one"}, None, {"two"}]) == [
        fd(),
        fd({"one": "one_0"}),
        fd({"one": "one_1"}),
        fd({"two": "two_0"}),
        fd({"one": "two_0", "two": "two_1"}),
        fd({"one": "two_1", "two": "two_2"}),
        fd({"one": "two_2", "two": "two_3"}),
        fd({"one": "two_3", "two": "two_4"}),
        fd({"one": "two_4"}),
    ]


def test_switch_default():
    @graph
    def switch_test(key: TS[str], value: TS[str]) -> TS[str]:
        return switch_(key, {DEFAULT: lambda v: const("one")}, value)

    assert eval_node(switch_test, ["one", "two"], ["test"]) == ["one", "one"]


def test_switch_no_output():
    @graph
    def switch_test(key: TS[str]):
        return switch_(key, {"one": lambda key: print_(key),
                             "two": lambda key: print_(key)})

    assert eval_node(switch_test, ["one", "two"]) is None


def test_switch_bundle():
    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def switch_test(key: TS[str]) -> TSB[AB]:
        return switch_(key, {
            "one": lambda key: TSB[AB].from_ts(a=1),
            "two": lambda key: TSB[AB].from_ts(b=1),
        })

    assert eval_node(switch_test, ["one", "two"]) == [{"a": 1}, {"b": 1}]


def test_switch_from_reduce():
    @graph
    def switch_test(key: TS[str], n: TSD[int, TS[int]]) -> TS[int]:
        no = reduce(add_, n)
        return switch_(key, {DEFAULT: lambda na, nb: na + nb}, no, no)

    assert eval_node(switch_test, ["o"], [{}, {1: 1}, {2: 2}, {3: 3, 4: 4, 5: 5}]) == [
        None,
        2,
        6,
        30,
    ]


def test_reduce_from_switch():
    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def switch_test(n: TSD[int, TS[int]]) -> TS[int]:
        refs = map_(
            lambda key, value: combine[TSB[AB]](
                a=value,
                b=switch_(value, {DEFAULT: lambda v: v + 1}, value),
            ),
            n,
        )
        return reduce(
            lambda x, y: combine[TSB[AB]](a=x.a + y.a, b=x.b + y.b),
            refs,
            combine[TSB[AB]](a=0, b=0),
        ).b

    assert eval_node(switch_test, [{}, {1: 1}, {2: 2}, {1: 2}, {2: 3}, {1: REMOVE}]) == [
        0,
        2,
        5,
        6,
        7,
        4,
    ]


def test_switch_bundle_from_reduce():
    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def switch_test(key: TS[str], n: TSD[int, TS[int]]) -> TS[int]:
        no = reduce(add_, n)
        return switch_(
            key,
            {DEFAULT: lambda value: value.a + value.b},
            combine[TSB[AB]](a=no, b=no),
        )

    assert eval_node(switch_test, ["o", None, None], [{}, {1: 1}, {2: 2}, {3: 3, 4: 4, 5: 5}]) == [
        None,
        2,
        6,
        30,
    ]


def test_switch_tss():
    @graph
    def switch_test(key: TS[str], value1: TSS[str], value2: TSS[str]) -> TSS[str]:
        selected = switch_(
            key,
            {
                "one": lambda v1, v2: dedup(v1),
                "two": lambda v1, v2: lag(v2, MIN_TD),
            },
            value1,
            value2,
        )
        return map_(lambda key: key, __keys__=selected).key_set

    assert eval_node(
        switch_test,
        ["one", None, "two", None],
        [{"a", "b"}, None, {Removed("a")}],
        [{"c", "d"}, None, None, {"e", "f"}],
    ) == [
        {"a", "b"},
        None,
        {Removed("a"), Removed("b")},
        {"c", "d"},
        {"e", "f"},
    ]


def test_switch_carries_tss_of_compound_scalars():
    from dataclasses import dataclass

    from hgraph import CompoundScalar, DEFAULT, emit, len_

    @dataclass(frozen=True)
    class Item(CompoundScalar):
        name: str

    @graph
    def one(values: TSS[Item]) -> TS[Item]:
        return emit(values)

    @graph
    def many(values: TSS[Item]) -> TS[Item]:
        return emit(values)

    @graph
    def switch_test(values: TSS[Item]) -> TS[Item]:
        return switch_(len_(values), {1: one, DEFAULT: many}, values)

    item = Item("one")
    assert eval_node(switch_test, [{item}]) == [item]


def test_switch_unifies_covariant_graph_outputs_at_the_declared_base_type():
    from dataclasses import dataclass

    from hgraph import CompoundScalar

    @dataclass(frozen=True)
    class Instrument(CompoundScalar, abstract=True):
        symbol: str

    @dataclass(frozen=True)
    class Future(Instrument):
        expiry: int

    @dataclass(frozen=True)
    class Option(Instrument):
        strike: float

    @graph
    def future() -> TS[Instrument]:
        return combine[TS[Future]](symbol="FUT", expiry=202612)

    @graph
    def option() -> TS[Instrument]:
        return combine[TS[Option]](symbol="OPT", strike=42.0)

    @graph
    def app(key: TS[str]) -> TS[Instrument]:
        return switch_(key, {"future": future, "option": option})

    assert eval_node(app, ["future", "option"]) == [
        Future(symbol="FUT", expiry=202612),
        Option(symbol="OPT", strike=42.0),
    ]


def test_switch_carries_tss_of_nested_catalogue_shaped_scalars():
    from dataclasses import dataclass

    from frozendict import frozendict
    from hgraph import CompoundScalar, DEFAULT, emit, len_

    @dataclass(frozen=True)
    class Store(CompoundScalar):
        path: str

    @dataclass(frozen=True)
    class Entry(CompoundScalar):
        schema: object
        dataset: str
        scope: frozendict[str, object]
        store: Store

    @dataclass(frozen=True)
    class Selection(CompoundScalar):
        dce: Entry
        options: frozendict[str, object]

    @graph
    def branch(values: TSS[Selection]) -> TS[Selection]:
        return emit(values)

    @graph
    def switch_test(values: TSS[Selection]) -> TS[Selection]:
        return switch_(len_(values), {1: branch, DEFAULT: branch}, values)

    selection = Selection(
        Entry(int, "rows", frozendict(), Store("sink")), frozendict())
    assert eval_node(switch_test, [{selection}]) == [selection]


def test_switch_branch_calls_service_adaptor_from_tss_key():
    from dataclasses import dataclass

    import hgraph as hg
    from hgraph import CompoundScalar, DEFAULT, emit, len_

    @dataclass(frozen=True)
    class Selection(CompoundScalar):
        value: int

    @hg.service_adaptor
    def adaptor(request: TS[Selection]) -> TS[int]: ...

    @hg.compute_node
    def extract(request: TS[Selection]) -> TS[int]:
        return request.value.value

    @hg.service_adaptor_impl(interfaces=adaptor)
    def adaptor_impl(requests: TSD[int, TS[Selection]]) -> TSD[int, TS[int]]:
        return map_(extract, request=requests)

    @graph
    def branch(values: TSS[Selection]) -> TS[int]:
        return adaptor(emit(values), path="switch-repro")

    @graph
    def switch_test(values: TSS[Selection]) -> TS[int]:
        hg.register_adaptor("switch-repro", adaptor_impl)
        return switch_(len_(values), {1: branch, DEFAULT: branch}, values)

    assert eval_node(switch_test, [{Selection(1)}]) == [None, 1]


def test_switch_branch_returns_service_adaptor_bundle_from_tss_key():
    from dataclasses import dataclass

    import hgraph as hg
    from hgraph import CompoundScalar, DEFAULT, TimeSeriesSchema, emit, len_

    @dataclass(frozen=True)
    class Selection(CompoundScalar):
        value: int

    class Reply(TimeSeriesSchema):
        status: TS[int]
        value: TS[int]

    @hg.service_adaptor
    def adaptor(request: TS[Selection]) -> TSB[Reply]: ...

    @graph
    def reply(request: TS[Selection]) -> TSB[Reply]:
        return hg.combine[TSB[Reply]](status=0, value=request.value)

    @hg.service_adaptor_impl(interfaces=adaptor)
    def adaptor_impl(requests: TSD[int, TS[Selection]]) -> TSD[int, TSB[Reply]]:
        delayed_requests = hg.feedback(TSD[int, TS[Selection]])
        delayed_requests(requests)
        return map_(reply, request=delayed_requests())

    @graph
    def branch(values: TSS[Selection]) -> TSB[Reply]:
        return adaptor(emit(values), path="switch-bundle-repro")

    @graph
    def switch_test(values: TSS[Selection]) -> TSB[Reply]:
        hg.register_adaptor("switch-bundle-repro", adaptor_impl)
        return switch_(len_(values), {1: branch, DEFAULT: branch}, values)

    assert eval_node(switch_test, [{Selection(1)}]) == [
        None, None, {"status": 0, "value": 1}]


def test_switch_service_adaptor_with_covariant_compound_field():
    from dataclasses import dataclass

    import hgraph as hg
    from hgraph import CompoundScalar, DEFAULT, TimeSeriesSchema, emit, len_

    @dataclass(frozen=True)
    class Store(CompoundScalar, abstract=True):
        path: str

    @dataclass(frozen=True)
    class ConcreteStore(Store):
        value: int

    @dataclass(frozen=True)
    class Selection(CompoundScalar):
        store: Store

    class Reply(TimeSeriesSchema):
        value: TS[int]

    @hg.service_adaptor
    def adaptor(request: TS[Selection]) -> TSB[Reply]: ...

    @graph
    def reply(request: TS[Selection]) -> TSB[Reply]:
        concrete = hg.downcast_ref(ConcreteStore, request.store)
        return hg.combine[TSB[Reply]](value=concrete.value)

    @hg.service_adaptor_impl(interfaces=adaptor)
    def adaptor_impl(requests: TSD[int, TS[Selection]]) -> TSD[int, TSB[Reply]]:
        delayed = hg.feedback(TSD[int, TS[Selection]])
        delayed(requests)
        return map_(reply, request=delayed())

    @graph
    def branch(values: TSS[Selection]) -> TSB[Reply]:
        return adaptor(emit(values), path="switch-covariant-repro")

    @graph
    def switch_test(values: TSS[Selection]) -> TSB[Reply]:
        hg.register_adaptor("switch-covariant-repro", adaptor_impl)
        return switch_(len_(values), {1: branch, DEFAULT: branch}, values)

    value = Selection(ConcreteStore("sink", 1))
    assert eval_node(switch_test, [{value}]) == [None, None, {"value": 1}]
