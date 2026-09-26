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
