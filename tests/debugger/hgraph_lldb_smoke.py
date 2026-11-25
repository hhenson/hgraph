"""Batch LLDB validation for the hgraph type-erasure printers."""

import json

import lldb


def _global(target, name):
    value = target.FindFirstGlobalVariable(name)
    if not value.IsValid():
        raise RuntimeError("missing debugger fixture global: {}".format(name))
    return value


def _summary(value):
    summary = value.GetSummary()
    if summary is None:
        raise RuntimeError("missing summary for {}".format(value.GetName()))
    if "<printer error:" in summary:
        raise RuntimeError(summary)
    return summary


def _require(text, expected):
    if expected not in text:
        raise RuntimeError("expected {!r} in {!r}".format(expected, text))


def _children(value):
    return {
        value.GetChildAtIndex(index).GetName(): value.GetChildAtIndex(index)
        for index in range(value.GetNumChildren())
    }


def _require_keyed_nested_graphs(value, label):
    children = _children(value)
    keys = [child for name, child in children.items() if name.startswith("key[")]
    graphs = [child for name, child in children.items() if name.startswith("value[")]
    if len(keys) != 2 or len(graphs) != 2:
        raise RuntimeError(
            "{} nested slots are incomplete: {}".format(label, sorted(children))
        )
    key_summaries = " ".join(_summary(child) for child in keys)
    _require(key_summaries, "value=22")
    _require(key_summaries, "value=33")
    if "value=11" in key_summaries:
        raise RuntimeError("{} retained an erased key: {}".format(label, key_summaries))
    for graph in graphs:
        _require(_summary(graph), "AnyPtr{")
        if "[0]" not in _children(graph):
            raise RuntimeError("{} child graph has no node navigation".format(label))


def _graph(debugger, exe_ctx, arguments=""):
    reply = lldb.SBCommandReturnObject()
    debugger.GetCommandInterpreter().HandleCommand(
        "hgraph-graph {}".format(arguments).strip(), exe_ctx, reply
    )
    if not reply.Succeeded():
        raise RuntimeError("hgraph-graph {} failed: {}".format(arguments, reply.GetError()))
    return json.loads(reply.GetOutput())


def _require_graph_report(report, label):
    if report.get("version") != 1 or "error" in report or report.get("label") != label:
        raise RuntimeError("hgraph-graph: expected node {}, got {}".format(label, report))
    return report


def _require_paused_graph(debugger, exe_ctx):
    # The fixture stops inside the sink's fourth evaluation.
    sink = _require_graph_report(_graph(debugger, exe_ctx), "debugger_nested_sink")
    inputs = {port["name"]: port for port in sink["inputs"]}
    sources = {name: [link["label"] for link in port["links"]] for name, port in inputs.items()}
    if sources != {"switched": ["switch_"], "mapped": ["map_"], "meshed": ["mesh_"]}:
        raise RuntimeError("hgraph-graph sink inputs are incomplete: {}".format(sink))
    for name in ("mapped", "meshed"):
        if inputs[name]["value"] != "<unavailable>":
            raise RuntimeError("hgraph-graph showed a collection as a scalar: {}".format(sink))

    snapshot = sink["snapshot"]
    switch = _require_graph_report(
        _graph(debugger, exe_ctx, "{} {}".format(snapshot, inputs["switched"]["links"][0]["node"])),
        "switch_",
    )
    if (switch["snapshot"], switch["stop"]) != (snapshot, sink["stop"]):
        raise RuntimeError("hgraph-graph changed snapshot within one stop: {}".format(switch))
    if not any(
        link["node"] == sink["node"] and link["port"] == "switched"
        for port in switch["outputs"]
        for link in port["links"]
    ):
        raise RuntimeError("hgraph-graph switch output does not reach the sink: {}".format(switch))

    # The switch reads the replayed value 4, which ticked at this evaluation.
    values = []
    for port in switch["inputs"]:
        for link in port["links"]:
            source = _graph(debugger, exe_ctx, "{} {}".format(snapshot, link["node"]))
            values.extend(output["value"] for output in source.get("outputs", ()))
    if "4 [modified]" not in values:
        raise RuntimeError("hgraph-graph switch sources show {}".format(values))

    stale = _graph(debugger, exe_ctx, "{} 0".format(snapshot + 1))
    if "error" not in stale:
        raise RuntimeError("hgraph-graph accepted another snapshot: {}".format(stale))


def run(debugger, _command, exe_ctx, result, _internal_dict):
    try:
        target = debugger.GetSelectedTarget()

        _require(_summary(_global(target, "fixture_atomic_pointer")), "value=42")
        _require(_summary(_global(target, "fixture_bundle_pointer")), "AnyPtr{DebuggerFixture")
        _require(_summary(_global(target, "fixture_map_pointer")), "AnyPtr{MutableMap[int32,int32]")
        _require(_summary(_global(target, "fixture_node_pointer")), "AnyPtr{debugger_fixture_node}")

        graph = _global(target, "fixture_graph_pointer")
        graph_summary = _summary(graph)
        _require(graph_summary, "AnyPtr{debugger_fixture_graph}")

        _require(_summary(_global(target, "fixture_typed_null_pointer")), "AnyPtr{int32 null}")
        _require(_summary(_global(target, "fixture_malformed_pointer")), "AnyPtr{malformed}")
        _require(_summary(_global(target, "fixture_invalid_record").Dereference()), "TypeRecord{invalid Value/Instance")
        _require(_summary(_global(target, "fixture_invalid_descriptor")), "DebugDescriptor{invalid layout=Atomic")

        bundle_children = _children(_global(target, "fixture_bundle_pointer"))
        if not {"record", "data", "number", "enabled"}.issubset(bundle_children):
            raise RuntimeError("bundle navigation is incomplete: {}".format(sorted(bundle_children)))
        _require(_summary(bundle_children["number"]), "value=42")

        map_children = _children(_global(target, "fixture_map_pointer"))
        if not {"key[0]", "value[0]"}.issubset(map_children):
            raise RuntimeError("map slot navigation is incomplete: {}".format(sorted(map_children)))

        graph_children = _children(graph)
        if "[0]" not in graph_children:
            raise RuntimeError("graph node navigation is incomplete: {}".format(sorted(graph_children)))
        _require(_summary(graph_children["[0]"]), "AnyPtr{debugger_fixture_graph_node}")

        nested_graph = _global(target, "fixture_nested_graph_pointer")
        _require(_summary(nested_graph), "AnyPtr{debugger_nested_graph}")

        switch_node = _global(target, "fixture_switch_node_pointer")
        _require(_summary(switch_node), "AnyPtr{switch_}")
        switch_children = _children(switch_node)
        if not {"graph[0]", "graph[1]"}.issubset(switch_children):
            raise RuntimeError("switch bank navigation is incomplete: {}".format(sorted(switch_children)))
        for name in ("graph[0]", "graph[1]"):
            graph = switch_children[name]
            _require(_summary(graph), "AnyPtr{")
            if "[0]" not in _children(graph):
                raise RuntimeError("switch bank {} has no node navigation".format(name))

        map_node = _global(target, "fixture_map_node_pointer")
        mesh_node = _global(target, "fixture_mesh_node_pointer")
        _require(_summary(map_node), "AnyPtr{map_}")
        _require(_summary(mesh_node), "AnyPtr{mesh_}")
        _require_keyed_nested_graphs(map_node, "map")
        _require_keyed_nested_graphs(mesh_node, "mesh")

        _require(_summary(_global(target, "fixture_value_view")), "ValueView{int32")
        _require(_summary(_global(target, "fixture_value_view")), "value=42")
        _require(_summary(_global(target, "fixture_value_type")), "ValueTypeRef{int32}")
        _require(_summary(_global(target, "fixture_ts_data_view")), "TSDataView{TS[")
        for name, expected in (("modified", "7 [modified]"), ("retained", "7"),
                               ("null", "null [modified]"), ("invalid", "<invalid>")):
            actual = _summary(_global(target, "fixture_" + name))
            if actual != expected:
                raise RuntimeError("{}: expected {}, got {}".format(name, expected, actual))

        input_view = _global(target, "fixture_ts_input_view")
        output_view = _global(target, "fixture_ts_output_view")
        _require(_summary(input_view), "TSInputView{")
        _require(_summary(output_view), "TSOutputView{TS[")
        _require(_summary(_global(target, "fixture_node_view")), "NodeView{")
        if not {"pointer", "input", "output", "graph"}.issubset(
            _children(_global(target, "fixture_node_view"))
        ):
            raise RuntimeError("NodeView endpoint navigation is incomplete")
        _require(_summary(_global(target, "fixture_graph_view")), "GraphView{debugger_nested_graph}")
        if not any(
            name.startswith("schedule[")
            for name in _children(_global(target, "fixture_graph_view"))
        ):
            raise RuntimeError("GraphView scheduling navigation is incomplete")
        if not {"source_data", "value_data", "raw_data", "consumer", "evaluation_time"}.issubset(_children(input_view)):
            raise RuntimeError("TSInputView navigation is incomplete")
        if not {"data", "owner", "owner_node", "evaluation_time", "delta"}.issubset(_children(output_view)):
            raise RuntimeError("TSOutputView navigation is incomplete")
        _require_paused_graph(debugger, exe_ctx)
    except Exception as error:
        result.SetError(str(error))
        return

    result.AppendMessage("hgraph LLDB type-erasure smoke test passed")


def __lldb_init_module(debugger, _internal_dict):
    debugger.HandleCommand("command script add -f hgraph_lldb_smoke.run hgraph-smoke")
