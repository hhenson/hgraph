from datetime import timedelta
from math import e

import pytest
from hgraph import TS, EvaluationMode, GraphConfiguration, convert, count, evaluate_graph, graph, compute_node, TSD, map_, schedule, switch_, try_except
from hgraph.debug import inspector
from hgraph.debug._inspector_item_id import InspectorItemId, InspectorItemType, NodeValueType
from hgraph.test import eval_node


def test_inspector_item_id():
    item_id = InspectorItemId(graph=(1, 2, 3))
    assert item_id.to_str() == "1.2.3"

    item_id = InspectorItemId(graph=(1, 2, 3), node=4)
    assert item_id.to_str() == "1.2.3:4"

    item_id = InspectorItemId(graph=(1, 2, 3), node=4, value_type=NodeValueType.Inputs, value_path=(5, 6, 7))
    assert item_id.to_str() == "1.2.3:4/INPUTS/5/6/7"

    item_id = InspectorItemId.from_str("1.2.3:4/INPUTS/5/6/7")
    assert item_id.item_type == InspectorItemType.Value
    assert item_id.graph == (1, 2, 3)
    assert item_id.node == 4
    assert item_id.value_type == NodeValueType.Inputs
    assert item_id.value_path == (5, 6, 7)

    item_id = InspectorItemId.from_str("1.2.3:4/")
    assert item_id.item_type == InspectorItemType.Node
    assert item_id.graph == (1, 2, 3)
    assert item_id.node == 4
    assert item_id.value_type == None
    assert item_id.value_path == ()

    item_id = InspectorItemId.from_str("1.2.3")
    assert item_id.item_type == InspectorItemType.Graph
    assert item_id.graph == (1, 2, 3)
    assert item_id.node == None
    assert item_id.value_type == None
    assert item_id.value_path == ()
    pass

    InspectorItemId.__reset__()

    item_id = InspectorItemId(graph=(1, 2, 3), node=4, value_type=NodeValueType.Inputs, value_path=(5, "6", 7))
    assert item_id.to_str() == "1.2.3:4/INPUTS/5/x001/7"

    item_id = InspectorItemId.from_str("1.2.3:4/INPUTS/5/x001/7")
    assert item_id.item_type == InspectorItemType.Value
    assert item_id.graph == (1, 2, 3)
    assert item_id.node == 4
    assert item_id.value_type == NodeValueType.Inputs
    assert item_id.value_path == (5, "6", 7)

    InspectorItemId.__reset__()


def test_inspector_sort_key():
    @compute_node
    def inspect_input_sort_key(i: TS[int]) -> TS[str]:
        return InspectorItemId(
            graph=i.owning_graph.graph_id,
            node=i.owning_node.node_ndx,
            value_type=NodeValueType.Inputs,
            value_path=("i",),
        ).sort_key()

    @graph
    def g1(i: TS[int]) -> TS[str]:
        return inspect_input_sort_key(i)

    InspectorItemId.__reset__()

    assert eval_node(g1, [1]) == ["001X01001"]

    InspectorItemId.__reset__()

    @graph
    def g2(i: TSD[int, TS[int]]) -> TSD[int, TS[str]]:
        return map_(inspect_input_sort_key, i)

    InspectorItemId.__reset__()

    assert eval_node(g2, [{1: 1}]) == [{1: "001X02001001X01001"}]

    InspectorItemId.__reset__()

    @graph
    def g3(i: TS[int]) -> TS[str]:
        return switch_(i, {1: inspect_input_sort_key}, i)

    InspectorItemId.__reset__()

    assert eval_node(g3, 1) == ["001X02001001X01001"]

    InspectorItemId.__reset__()

    @graph
    def g4_helper(i: TS[int]) -> TS[str]:
        return inspect_input_sort_key(i)

    @graph
    def g4(i: TS[int]) -> TS[str]:
        return try_except(g4_helper, i).out

    InspectorItemId.__reset__()

    assert eval_node(g4, 1) == ["001X02000001X01001"]

    InspectorItemId.__reset__()


@pytest.mark.skip(reason="Requires interactive inspection; not suitable for automated testing.")
def test_run_inspector():
    @graph
    def g() -> TSD[int, TS[int]]:
        inspector(8888)
        
        ticks = schedule(timedelta(seconds=1))
        tsd = convert[TSD[int, TS[int]]](key=count(ticks), ts=count(ticks))
        mapped = map_(lambda x: x * 2, tsd)
        return mapped
    
    result = evaluate_graph(g, config=GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=5)))
    assert result