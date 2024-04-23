from hgraph import compute_node, CONTEXT, TS, graph, switch_, REQUIRED, TSD, map_, pass_through
from hgraph.nodes import const, format_
from hgraph.test import eval_node


class TestContext:
    __instance__ = None

    def __init__(self, msg: str):
        self.msg = msg

    @classmethod
    def instance(cls):
        if cls.__instance__ is None:
            raise ValueError("No instance of TestContext available.")
        return cls.__instance__

    def __enter__(self):
        type(self).__instance__ = self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if type(self).__instance__ == self:
            type(self).__instance__ = None
        else:
            raise ValueError("Exiting context not entered.")


def test_context_0():
    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[TestContext]] = None) -> TS[str]:
        return f"{TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        with const(TestContext("Hello")):
            return use_context(ts)

    assert eval_node(g, [True, None, False], __trace__=True) == ["Hello True", None, "Hello False"]


def test_context_1():
    @compute_node
    def create_context(msg: TS[str]) -> TS[TestContext]:
        return TestContext(msg.value)

    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[TestContext]] = None) -> TS[str]:
        return f"{TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool], s: TS[str]) -> TS[str]:
        with create_context(format_("{}_", s)):  # Now the context node would rank lower than use_context if they were not linked
            return use_context(ts)

    assert eval_node(g, ts=[True, None, False], s=['Hello', None, 'Hulla'], __trace__=True) == ["Hello_ True", None, "Hulla_ False"]


def test_context_2():
    @compute_node
    def create_context(msg: TS[str]) -> TS[TestContext]:
        return TestContext(msg.value)

    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[TestContext]] = REQUIRED) -> TS[str]:
        return f"{TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool], s: TS[str]) -> TS[str]:
        with create_context(format_("{}_", s)):  # Now the context node would rank lower than use_context if they were not linked
            return switch_({True: lambda t: use_context(t), False: lambda t: format_('Chao {}', t)}, ts, ts)

    assert eval_node(g, ts=[True, None, False], s=['Hello', None, 'Hulla']) == ["Hello_ True", None, "Chao False"]


def test_context_3():
    @compute_node
    def create_context(msg: TS[str]) -> TS[TestContext]:
        return TestContext(msg.value)

    @compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[TestContext]] = REQUIRED) -> TS[str]:
        return f"{TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool], s: TS[str]) -> TS[str]:
        with create_context(format_("{}_", s)):  # Now the context node would rank lower than use_context if they were not linked
            return switch_({True: lambda t: use_context(t), False: lambda t: format_('Chao {}', t)}, ts, ts)

    @graph
    def f(ts: TSD[int, TS[bool]], s: TSD[int, TS[str]]) -> TSD[int, TS[str]]:
        return map_(g, ts, s)

    @graph
    def h(ts: TSD[int, TSD[int, TS[bool]]], s: TSD[int, TS[str]]) -> TSD[int, TSD[int, TS[str]]]:
        return map_(f, ts, pass_through(s))

    assert eval_node(h, ts=[{1: {1: True}, 2: {2: True}}, {1: {1: False}}, None], s=[{1: 'Hello', 2: "Chao"}, None, {2: "Ho"}], __trace__=True) \
            == [{1: {1: "Hello_ True"}, 2: {2: "Chao_ True"}}, {1: {1: "Chao False"}}, {2: {2: "Ho_ True"}}]
