from ctypes import cast
from dataclasses import dataclass
from datetime import timedelta
from math import e
from socket import gethostname
from types import SimpleNamespace
from typing import Callable, Tuple

import pytest
from hgraph import (
    REF,
    STATE,
    TS,
    TSB,
    EvaluationMode,
    GlobalState,
    GraphConfiguration,
    CompoundScalar,
    TimeSeriesReference,
    TimeSeriesSchema,
    combine,
    convert,
    count,
    debug_print,
    evaluate_graph,
    graph,
    compute_node,
    TSD,
    if_true,
    is_feature_enabled,
    map_,
    push_queue,
    register_adaptor,
    schedule,
    sink_node,
    stop_engine,
    switch_,
    try_except,
)
from hgraph.adaptors.perspective import PerspectiveTablesManager, perspective_web
from hgraph.adaptors.tornado import HttpGetRequest, HttpPostRequest, http_client_adaptor, http_client_adaptor_impl
from hgraph.debug import inspector
from hgraph.debug._inspector_handler import inspector_read_value
from hgraph.debug._inspector_item_id import InspectorItemId, InspectorItemType, NodeValueType
from hgraph.test import eval_node

HAS_CPP_RUNTIME = is_feature_enabled("use_cpp")
if HAS_CPP_RUNTIME:
    from hgraph._hgraph import NestedNode


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


@pytest.mark.skipif(
    __import__('shutil').which('npm') is None,
    reason="npm is not installed (required for Perspective web components)"
)
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


@pytest.mark.skipif(not HAS_CPP_RUNTIME, reason="Current engine is not C++")
def test_cpp_reduce_nested_graphs_are_exposed():
    from hgraph import add_, reduce

    @compute_node
    def reduce_nested_graph_count(ts: TS[int]) -> TS[int]:
        for node in ts.owning_graph.nodes:
            if isinstance(node, NestedNode) and node.signature.name == "reduce":
                return len(node.nested_graphs)
        return -1

    @graph
    def g() -> TS[int]:
        ticks = schedule(timedelta(milliseconds=1))
        tsd = convert[TSD[int, TS[int]]](key=count(ticks), ts=count(ticks))
        out = reduce(add_, tsd, 0)
        return reduce_nested_graph_count(out)

    result = evaluate_graph(
        g,
        GraphConfiguration(
            run_mode=EvaluationMode.SIMULATION,
            end_time=timedelta(milliseconds=2),
        ),
    )

    assert [v for _, v in result] == [1, 1], result


def test_inspector_reads_nested_ref_tsd_values():
    @compute_node
    def make_ref(tsd: TSD[int, TS[int]]) -> REF[TSD[int, TS[int]]]:
        return TimeSeriesReference.make(tsd.output)

    @compute_node
    def wrap_ref(ref: REF[TSD[int, TS[int]]]) -> REF[TSD[int, TS[int]]]:
        return TimeSeriesReference.make(ref.output)

    @compute_node
    def inspect_ref_value(ref: REF[TSD[int, TS[int]]]) -> TS[int]:
        graph = ref.owning_graph
        state = SimpleNamespace(
            observer=SimpleNamespace(
                get_graph_info=lambda graph_id: SimpleNamespace(graph=graph) if graph_id == graph.graph_id else None
            )
        )
        body, _ = inspector_read_value(state, InspectorItemId.from_object(ref.output))
        return len(body)

    @graph
    def g(tsd: TSD[int, TS[int]]) -> TS[int]:
        return inspect_ref_value(wrap_ref(make_ref(tsd)))

    values = eval_node(g, [{1: 1}, {1: 2}], __elide__=True)
    assert values and all(value > 0 for value in values), values


def test_inspector_preserves_partial_ref_bundle_values():
    import pyarrow as pa

    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    def read_rows(ts):
        graph = ts.owning_graph
        state = SimpleNamespace(
            observer=SimpleNamespace(
                get_graph_info=lambda graph_id: SimpleNamespace(graph=graph) if graph_id == graph.graph_id else None
            )
        )
        body, _ = inspector_read_value(state, InspectorItemId.from_object(ts.output if ts.output is not None else ts))
        return pa.ipc.RecordBatchStreamReader(body).read_all().to_pylist()

    @compute_node
    def partial_ref_bundle(i: TS[int]) -> REF[TSB[AB]]:
        return TimeSeriesReference.make(from_items=[TimeSeriesReference.make(i.output), TimeSeriesReference.make()])

    @compute_node
    def partial_tsd_ref_bundle(i: TS[int]) -> TSD[str, REF[TSB[AB]]]:
        return {"k": TimeSeriesReference.make(from_items=[TimeSeriesReference.make(i.output), TimeSeriesReference.make()])}

    @compute_node
    def make_ref_tsd(tsd: TSD[str, REF[TSB[AB]]]) -> REF[TSD[str, REF[TSB[AB]]]]:
        return TimeSeriesReference.make(tsd.output)

    @compute_node
    def inspect_ref_bundle(ref: REF[TSB[AB]]) -> TS[object]:
        return read_rows(ref)

    @compute_node
    def inspect_tsd_ref_bundle(tsd: TSD[str, REF[TSB[AB]]]) -> TS[object]:
        return read_rows(tsd)

    @compute_node
    def inspect_ref_tsd_ref_bundle(ref: REF[TSD[str, REF[TSB[AB]]]]) -> TS[object]:
        return read_rows(ref)

    @graph
    def g_ref(i: TS[int]) -> TS[object]:
        return inspect_ref_bundle(partial_ref_bundle(i))

    @graph
    def g_tsd(i: TS[int]) -> TS[object]:
        return inspect_tsd_ref_bundle(partial_tsd_ref_bundle(i))

    @graph
    def g_ref_tsd(i: TS[int]) -> TS[object]:
        return inspect_ref_tsd_ref_bundle(make_ref_tsd(partial_tsd_ref_bundle(i)))

    assert eval_node(g_ref, [1], __elide__=True) == [[{"a": 1, "b": None}]]
    assert eval_node(g_tsd, [1], __elide__=True) == [
        [{"__key_1_removed__": False, "__key_1__": "k", "a": 1, "b": None}]
    ]
    assert eval_node(g_ref_tsd, [1], __elide__=True) == [
        [{"__key_1_removed__": False, "__key_1__": "k", "a": 1, "b": None}]
    ]


def test_inspector_reads_tsd_output_child_ref_bundle_with_nested_tsd():
    import pyarrow as pa

    @dataclass(frozen=True)
    class Req(CompoundScalar):
        sym: str

    class Inner(TimeSeriesSchema):
        data: TSD[str, TS[str]]
        name: TS[str]

    @compute_node
    def make_bundle(name: TS[str]) -> TSB[Inner]:
        return {"data": {"k": name.value}, "name": name.value}

    @compute_node
    def make_tsd(bundle: TSB[Inner]) -> TSD[Req, REF[TSB[Inner]]]:
        return {Req("x"): TimeSeriesReference.make(bundle.output)}

    @compute_node
    def inspect_output_child(tsd: TSD[Req, REF[TSB[Inner]]]) -> TS[object]:
        graph = tsd.owning_graph
        state = SimpleNamespace(
            observer=SimpleNamespace(
                get_graph_info=lambda graph_id: SimpleNamespace(graph=graph) if graph_id == graph.graph_id else None
            )
        )
        body, _ = inspector_read_value(state, InspectorItemId.from_object(tsd.output[Req("x")]))
        return pa.ipc.RecordBatchStreamReader(body).read_all().to_pylist()

    @graph
    def g(name: TS[str]) -> TS[object]:
        return inspect_output_child(make_tsd(make_bundle(name)))

    assert eval_node(g, ["z"], __elide__=True) == [
        [{"data.__key_1_removed__": False, "data.__key_1__": "k", "data.value": "z", "name": "z"}]
    ]
