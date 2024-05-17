from frozendict import frozendict

from hgraph import switch_, graph, TS, SCALAR, compute_node, generator, EvaluationClock, MIN_TD, TSD, TSS, map_, DEFAULT
from hgraph.nodes import add_ts, sub_ts, const, default, print_
from hgraph.test import eval_node


def test_switch():
    @graph
    def switch_test(key: TS[str], lhs: TS[int], rhs: TS[int]) -> TS[int]:
        s = switch_({'add': add_ts, 'sub': sub_ts}, key, lhs, rhs)
        return s

    assert eval_node(switch_test, ['add', 'sub'], [1, 2], [3, 4]) == [4, -2]


def test_switch_with_graph():
    @graph
    def graph_1(value: SCALAR) -> TS[SCALAR]:
        return const(f"{value}_1")

    @graph
    def graph_2(value: SCALAR) -> TS[SCALAR]:
        return const(f"{value}_2")

    @graph
    def switch_test(key: TS[str], value: SCALAR) -> TS[SCALAR]:
        return switch_({'one': graph_1, 'two': graph_2}, key, value)

    assert eval_node(switch_test, ['one', 'two'], "test") == ["test_1", "test_2"]


STARTED = 0
STOPPED = 0


def test_stop_start():
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
        return switch_({'one': g, 'two': g}, key)

    assert eval_node(switch_test, ['one', 'two']) == ["one", "two"]

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
    return switch_({'one': one_, 'two': two_}, key)


@graph
def _map(keys: TSS[str]) -> TSD[str, TS[str]]:
    return map_(_switch, __keys__=keys, __key_arg__='key')


def test_nested_switch():
    fd = frozendict
    assert eval_node(_map, [{"one"}, None, {"two"}]) == [
        fd(),
        fd({"one": "one_0"}),
        fd({"one": "one_1"}),
        fd({"two": "two_0"}),
        fd({'one': 'two_0', 'two': 'two_1'}),
        fd({'one': 'two_1', 'two': 'two_2'}),
        fd({'one': 'two_2', 'two': 'two_3'}),
        fd({'one': 'two_3', 'two': 'two_4'}),
        fd({'one': 'two_4'}),
    ]


def test_switch_default():
    @graph
    def switch_test(key: TS[str], value: TS[str]) -> TS[str]:
        return switch_({DEFAULT: lambda v: const("one")}, key, value)

    assert eval_node(switch_test, ['one', 'two'], ["test"]) == ["one", "one"]


def test_switch_no_output():
    @graph
    def switch_test(key: TS[str]):
        return switch_({'one': lambda key: print_(key), 'two': lambda key: print_(key)}, key)

    assert eval_node(switch_test, ['one', 'two']) == None