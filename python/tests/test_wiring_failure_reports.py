"""A failed wiring call says where: the graphs that led to it (runtime spec WIR-4)."""

import pytest

import hgraph as hg
from hgraph import TS, TSD, graph
from hgraph.test import eval_node


def test_a_wrapped_callable_names_itself_when_its_result_cannot_be_lifted():
    # map_ wraps the lambda as a child graph; the lambda's plain result is
    # lifted to a const inside the child's scope, so when the lift fails the
    # report names the child as well as the graph that called map_.
    @graph
    def maps_lambda(xs: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return hg.map_(lambda x: ({1: 2}, {"a": "b"}), xs)

    with pytest.raises(hg.WiringError) as error:
        eval_node(maps_lambda, [{"a": 1}])
    message = str(error.value)
    assert "wiring path: <lambda>" in message
    assert message.rstrip().endswith("wiring path: maps_lambda")


@pytest.mark.parametrize("runner", [eval_node, hg.run_graph])
@pytest.mark.parametrize("sink", [False, True])
def test_deferred_service_failure_keeps_the_root_graph_path(runner, sink):
    @hg.operator
    def only_integer(value: TS[int]) -> TS[int]: ...

    @hg.compute_node(overloads=only_integer)
    def integer_impl(value: TS[int]) -> TS[int]:
        return value.value

    @hg.reference_service
    def deferred_value(path: str = "deferred") -> TS[int]: ...

    @hg.service_impl(interfaces=deferred_value)
    def broken_service() -> TS[int]:
        return only_integer(hg.const("wrong type"))

    @graph
    def deferred_output_root() -> TS[int]:
        hg.register_service("deferred", broken_service)
        return deferred_value()

    @graph
    def deferred_sink_root():
        hg.register_service("deferred", broken_service)
        hg.debug_print("value", deferred_value())

    root = deferred_sink_root if sink else deferred_output_root
    with pytest.raises(hg.WiringError) as error:
        runner(root)
    message = str(error.value)
    assert "no matching overload for operator 'only_integer'" in message
    assert f"wiring path: {root.__name__}" in message
    assert message.count(root.__name__) == 1

    @graph
    def healthy() -> TS[int]:
        return hg.const(7)

    assert eval_node(healthy) == [7]
