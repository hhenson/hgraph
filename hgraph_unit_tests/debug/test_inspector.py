from ctypes import cast
from datetime import timedelta
from math import e
from socket import gethostname
from typing import Callable, Tuple

import pytest
from hgraph import STATE, TS, TSB, EvaluationMode, GlobalState, GraphConfiguration, TimeSeriesSchema, combine, convert, count, debug_print, evaluate_graph, graph, compute_node, TSD, if_true, map_, push_queue, register_adaptor, schedule, sink_node, stop_engine, switch_, try_except
from hgraph.adaptors.perspective import PerspectiveTablesManager, perspective_web
from hgraph.adaptors.tornado import HttpGetRequest, HttpPostRequest, http_client_adaptor, http_client_adaptor_impl
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


def test_run_inspector():
    import polars as pl
    import pyarrow
    
    @graph
    def g() -> TSD[int, TS[int]]:
        inspector(8888)
        perspective_web(gethostname(), port=8888)
        
        ticks = schedule(timedelta(milliseconds=10))
        tsd = convert[TSD[int, TS[int]]](key=count(ticks), ts=count(ticks))
        mapped = map_(lambda x: x * 2, tsd)
        
        @push_queue(TS[pl.DataFrame])
        def table_updates(sender: Callable, table: str) -> TS[pl.DataFrame]:
            def on_update(data):
                print("Table update received for", table)
                df = pl.from_arrow(pyarrow.RecordBatchStreamReader(data).read_all(), schema_overrides={'status': pl.String})
                if len(df) > 0:
                    try:
                        df = df.with_columns(date=pl.col("time").cast(pl.Date))
                        sender(df)
                    except Exception as e:
                        ...

            manager = PerspectiveTablesManager.current()
            manager.subscribe_table_updates(table, on_update, self_updates=True)
            print("Subscribed to table updates for", table)

        class TestInspectorBundle(TimeSeriesSchema):
            requests: TS[str]
            done: TS[bool]

        @compute_node
        def test_inspector(updates: TS[pl.DataFrame], _state: STATE = None) -> TSB[TestInspectorBundle]:
            if not hasattr(_state, 'df'):
                _state.df = updates.value
            else:
                _state.df = _state.df.update(updates.value, on='id', how='full')
                
            u = _state.df
            print(u)
            
            if getattr(_state, 'test_map', None) is None:
                f = u.filter(pl.col("type") == "MAP")
                if len(f) > 0:
                    i = f['id'][0]
                    _state.test_map = False
                    _state.map_id = i
                    return {'requests': f"expand/{i}"}
                
            if getattr(_state, 'test_map', None) is False:
                f = u.filter(pl.col("id").cast(str).str.starts_with(_state.map_id + '/'))
                if len(f) == 3:
                    print("checking MAP", f, f['name'].cast(str).str.strip_chars().is_in(["INPUTS", "OUTPUT", "GRAPHS"]))
                    _state.test_map = f['name'].cast(str).str.strip_chars().is_in(["INPUTS", "OUTPUT", "GRAPHS"]).all()
                    
                    f = f.filter(pl.col("name").cast(str).str.strip_chars() == "GRAPHS")
                    if len(f) > 0:
                        o = f['ord'][0]
                        i = f['id'][0]
                        _state.test_graphs = False
                        _state.graphs_ord = o
                        return {'requests': f"expand/{i}"}
                
            if getattr(_state, 'test_graphs', None) is False:
                f = u.filter(pl.col("ord").cast(str).str.starts_with(_state.graphs_ord + '0'))
                if len(f) > 0:
                    print("checking GRAPHS", f, f['type'].cast(str).str.strip_chars().is_in(["GRAPH"]))
                    _state.test_graphs = f['type'].cast(str).str.strip_chars().is_in(["GRAPH"]).all()
                
            if getattr(_state, 'test_push', None) is None:
                f = u.filter(pl.col("type") == "PUSH_SOURCE")
                if len(f) > 0:
                    i = f['id'][0]
                    _state.test_push = False
                    _state.push_id = i
                    return {'requests': f"expand/{i}"}
                
            if getattr(_state, 'test_push', None) is False:
                f = u.filter(pl.col("id").cast(str).str.starts_with(_state.push_id + '/'))
                if len(f) == 1:
                    print("checking PUSH", f, f['name'].cast(str).str.strip_chars().is_in(["OUTPUT"]))
                    _state.test_push = f['name'].cast(str).str.strip_chars().is_in(["OUTPUT"]).all()
                    
            if getattr(_state, 'test_sink', None) is None:
                f = u.filter((pl.col("type") == "SINK") & (pl.col("name").cast(str).str.contains("debug_print")))
                if len(f) > 0:
                    i = f['id'][0]
                    _state.test_sink = False
                    _state.sink_id = i
                    return {'requests': f"expand/{i}"}
                
            if getattr(_state, 'test_sink', None) is False:
                f = u.filter(pl.col("id").cast(str).str.starts_with(_state.sink_id + '/'))
                if len(f) == 2:
                    print("checking SINK", f, f['name'].cast(str).str.strip_chars().is_in(["SCALARS", "INPUTS"]))
                    _state.test_sink = f['name'].cast(str).str.strip_chars().is_in(["SCALARS", "INPUTS"]).all()
                    
            return {'done': 
                getattr(_state, 'test_map', False) 
                and 
                getattr(_state, 'test_push', False)
                and 
                getattr(_state, 'test_sink', False)
                and 
                getattr(_state, 'test_graphs', False)
            }
                

        @test_inspector.start
        def test_inspector_start(_state: STATE = None):
            GlobalState.instance().test_state = _state


        test = test_inspector(table_updates("inspector"))
        debug_print("requests", http_client_adaptor(combine[TS[HttpGetRequest]](url="http://localhost:8888/inspect/" + test.requests)).status_code)
        debug_print("expand", http_client_adaptor(HttpGetRequest(url="http://localhost:8888/inspect/expand/")).status_code)
        
        stop_engine(if_true(test.done))
        
        register_adaptor(None, http_client_adaptor_impl)
        
        return mapped
    
    with GlobalState() as gs, pl.StringCache():
        result = evaluate_graph(g, config=GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=5), trace=False))
    
        assert gs.test_state.test_map
        assert gs.test_state.test_push
        assert gs.test_state.test_sink
        assert gs.test_state.test_graphs
        
        
def test_inspector_graph_api_graph_id():
    @compute_node
    def inspect_graph_id(g: TS[bool]) -> TS[Tuple[int, ...]]:
        return g.owning_graph.graph_id
    
    assert eval_node(inspect_graph_id, [True]) == [()]

    
def test_inspector_graph_api_node_id():
    @compute_node
    def test_node_id(g: TS[bool]) -> TS[Tuple[int, ...]]:
        return g.owning_node.node_id
    
    assert eval_node(test_node_id, [True]) == [ (1,) ]
    
def test_inspector_graph_api_key_from_value():
    @compute_node
    def test_key_from_value(g: TS[bool]) -> TS[str]:
        key = g.parent_input.key_from_value(g)
        return key
    
    assert eval_node(test_key_from_value, [True]) == ['g']

    
def test_inspector_graph_api_key_from_value_tsd():
    @compute_node
    def test_key_from_value_tsd(g: TSD[int, TS[bool]]) -> TSD[int, TS[int]]:
        return {k: i.parent_input.key_from_value(i) for k, i in g.modified_items()}
    
    assert eval_node(test_key_from_value_tsd, [{1: True, 2: False}]) == [{1: 1, 2: 2}]
