"""Batch GDB validation for the hgraph type-erasure printers."""

import gdb


def _printer(expression):
    value = gdb.parse_and_eval(expression)
    printer = gdb.default_visualizer(value)
    if printer is None:
        raise gdb.GdbError("missing printer for {}".format(expression))
    return value, printer


def _summary(expression):
    _, printer = _printer(expression)
    summary = str(printer.to_string())
    if "<printer error:" in summary:
        raise gdb.GdbError(summary)
    return summary


def _require(text, expected):
    if expected not in text:
        raise gdb.GdbError("expected {!r} in {!r}".format(expected, text))


def _children(expression):
    _, printer = _printer(expression)
    return {name: value for name, value in printer.children()}


def _child_summary(value):
    printer = gdb.default_visualizer(value)
    if printer is None:
        raise gdb.GdbError("missing child printer")
    summary = str(printer.to_string())
    if "<printer error:" in summary:
        raise gdb.GdbError(summary)
    return summary


def _require_keyed_nested_graphs(expression, label):
    children = _children(expression)
    keys = [child for name, child in children.items() if name.startswith("key[")]
    graphs = [child for name, child in children.items() if name.startswith("value[")]
    if len(keys) != 2 or len(graphs) != 2:
        raise gdb.GdbError(
            "{} nested slots are incomplete: {}".format(label, sorted(children))
        )
    key_summaries = " ".join(_child_summary(child) for child in keys)
    _require(key_summaries, "value=22")
    _require(key_summaries, "value=33")
    if "value=11" in key_summaries:
        raise gdb.GdbError("{} retained an erased key: {}".format(label, key_summaries))
    for graph in graphs:
        _require(_child_summary(graph), "AnyPtr{")
        graph_printer = gdb.default_visualizer(graph)
        graph_children = {name: child for name, child in graph_printer.children()}
        if "[0]" not in graph_children:
            raise gdb.GdbError("{} child graph has no node navigation".format(label))


class HGraphSmoke(gdb.Command):
    def __init__(self):
        super().__init__("hgraph-smoke", gdb.COMMAND_USER)

    def invoke(self, _argument, _from_tty):
        _require(_summary("fixture_atomic_pointer"), "value=42")
        _require(_summary("fixture_string_pointer"), "value='debugger string'")
        _require(_summary("fixture_bundle_pointer"), "AnyPtr{DebuggerFixture")
        _require(_summary("fixture_bundle_pointer"), "fields=2")
        _require(_summary("fixture_set_pointer"), "size=2")
        _require(_summary("fixture_map_pointer"), "AnyPtr{MutableMap[int32,int32]")
        _require(_summary("fixture_map_pointer"), "size=1")
        _require(_summary("fixture_node_pointer"), "AnyPtr{debugger_fixture_node}")

        graph_summary = _summary("fixture_graph_pointer")
        _require(graph_summary, "AnyPtr{debugger_fixture_graph}")

        _require(_summary("fixture_typed_null_pointer"), "AnyPtr{int32 null}")
        _require(_summary("fixture_malformed_pointer"), "AnyPtr{malformed}")
        _require(_summary("*fixture_invalid_record"), "TypeRecord{invalid Value/Instance")
        _require(_summary("fixture_invalid_descriptor"), "DebugDescriptor{invalid layout=Atomic")

        bundle_children = _children("fixture_bundle_pointer")
        if not {"record", "data", "number", "enabled"}.issubset(bundle_children):
            raise gdb.GdbError("bundle navigation is incomplete: {}".format(sorted(bundle_children)))
        number_printer = gdb.default_visualizer(bundle_children["number"])
        _require(str(number_printer.to_string()), "value=42")

        map_children = _children("fixture_map_pointer")
        if not {"key[0]", "value[0]"}.issubset(map_children):
            raise gdb.GdbError("map slot navigation is incomplete: {}".format(sorted(map_children)))
        set_children = _children("fixture_set_pointer")
        if not {"[0]", "[1]"}.issubset(set_children):
            raise gdb.GdbError("set navigation is incomplete: {}".format(sorted(set_children)))

        graph_children = _children("fixture_graph_pointer")
        if "[0]" not in graph_children:
            raise gdb.GdbError("graph node navigation is incomplete: {}".format(sorted(graph_children)))
        node_printer = gdb.default_visualizer(graph_children["[0]"])
        _require(str(node_printer.to_string()), "AnyPtr{debugger_fixture_graph_node}")

        _require(_summary("fixture_nested_graph_pointer"), "AnyPtr{debugger_nested_graph}")
        _require(_summary("fixture_switch_node_pointer"), "AnyPtr{switch_}")
        switch_children = _children("fixture_switch_node_pointer")
        if not {"graph[0]", "graph[1]"}.issubset(switch_children):
            raise gdb.GdbError(
                "switch bank navigation is incomplete: {}".format(sorted(switch_children))
            )
        for name in ("graph[0]", "graph[1]"):
            graph = switch_children[name]
            _require(_child_summary(graph), "AnyPtr{")
            graph_printer = gdb.default_visualizer(graph)
            graph_children = {child_name: child for child_name, child in graph_printer.children()}
            if "[0]" not in graph_children:
                raise gdb.GdbError("switch bank {} has no node navigation".format(name))

        _require(_summary("fixture_map_node_pointer"), "AnyPtr{map_}")
        _require(_summary("fixture_mesh_node_pointer"), "AnyPtr{mesh_}")
        _require_keyed_nested_graphs("fixture_map_node_pointer", "map")
        _require_keyed_nested_graphs("fixture_mesh_node_pointer", "mesh")

        _require(_summary("fixture_value_view"), "ValueView{int32")
        _require(_summary("fixture_value_view"), "value=42")
        _require(_summary("fixture_value_type"), "ValueTypeRef{int32}")
        _require(_summary("fixture_ts_data_view"), "TSDataView{TS[")
        _require(_summary("fixture_ts_input_view"), "TSInputView{")
        _require(_summary("fixture_ts_output_view"), "TSOutputView{TS[")
        _require(_summary("fixture_tss_view"), "size=2")
        _require(_summary("fixture_tsd_view"), "size=1")
        _require(_summary("fixture_tsb_view"), "fields=2")
        if not {"value", "[0]", "[1]", "last_modified"}.issubset(_children("fixture_tss_view")):
            raise gdb.GdbError("TSS current-value navigation is incomplete")
        if not {"value", "key[0]", "value[0]", "last_modified"}.issubset(
            _children("fixture_tsd_view")
        ):
            raise gdb.GdbError("TSD current-value navigation is incomplete")
        if not {"value", "number", "enabled", "last_modified"}.issubset(_children("fixture_tsb_view")):
            raise gdb.GdbError("TSB current-value navigation is incomplete")
        _require(_child_summary(_children("fixture_tsb_view")["number"]), "AnyPtr{TS[int32]")
        _require(_summary("fixture_node_view"), "NodeView{")
        node_view_children = _children("fixture_node_view")
        if not {"pointer", "input", "output", "graph"}.issubset(node_view_children):
            raise gdb.GdbError("NodeView endpoint navigation is incomplete")
        node_pointer_printer = gdb.default_visualizer(node_view_children["pointer"])
        if node_pointer_printer is None or not {"input", "output"}.issubset(
            dict(node_pointer_printer.children())
        ):
            raise gdb.GdbError("normal node pointer printer has no input/output navigation")
        _require(_summary("fixture_graph_view"), "GraphView{debugger_nested_graph}")
        if not any(name.startswith("schedule[") for name in _children("fixture_graph_view")):
            raise gdb.GdbError("GraphView scheduling navigation is incomplete")
        if not {"source_data", "value_data", "raw_data", "consumer", "evaluation_time"}.issubset(
            _children("fixture_ts_input_view")
        ):
            raise gdb.GdbError("TSInputView navigation is incomplete")
        if not {"data", "owner", "owner_node", "evaluation_time", "delta"}.issubset(
            _children("fixture_ts_output_view")
        ):
            raise gdb.GdbError("TSOutputView navigation is incomplete")
        if not any(name.startswith("subscriber[") for name in _children("fixture_ts_output_view")):
            raise gdb.GdbError("TSOutputView subscriber navigation is incomplete")
        subscriber_value = next(
            value
            for name, value in _children("fixture_ts_output_view").items()
            if name.startswith("subscriber[")
        )
        subscriber_printer = gdb.default_visualizer(subscriber_value)
        if subscriber_printer is None:
            raise gdb.GdbError("subscriber has no normal pretty-printer")
        subscriber_children = dict(subscriber_printer.children())
        if "notifies" not in subscriber_children:
            raise gdb.GdbError("subscriber normal printer has no notification target")
        target_printer = gdb.default_visualizer(subscriber_children["notifies"])
        if target_printer is None or "node" not in dict(target_printer.children()):
            raise gdb.GdbError("notification target normal printer has no node")
        subscriber = gdb.execute(
            "hg-p fixture_ts_output_view subscriber[0]",
            to_string=True,
        )
        _require(subscriber, "TSInputTargetLinkState")
        if "owner:" not in subscriber and "target:" not in subscriber:
            raise gdb.GdbError("subscriber concrete object was not shallowly expanded")
        notified_node = gdb.execute(
            "hg-p fixture_ts_output_view subscriber[0] notifies node",
            to_string=True,
        )
        _require(notified_node, "debugger_nested_sink")
        notified_input = gdb.execute(
            "hg-p fixture_ts_output_view subscriber[0] notifies node input pointer",
            to_string=True,
        )
        for field in ("switched:", "mapped:", "meshed:"):
            _require(notified_input, field)
        _require(
            gdb.execute(
                "hg-p fixture_ts_output_view subscriber[0] notifies node input pointer switched",
                to_string=True,
            ),
            "AnyPtr{TS[int]",
        )

        navigated = gdb.execute(
            "hg-p fixture_ts_output_view owning_node graph nodes 4 output",
            to_string=True,
        )
        _require(navigated, "TSOutput{TS[int]")
        _require(
            gdb.execute("hg-p fixture_derived_output_view", to_string=True),
            "TSOutputView{TS[int]",
        )
        node_listing = gdb.execute("hg-p fixture_node_view", to_string=True)
        _require(node_listing, "input:")
        _require(node_listing, "output:")
        gdb.execute("hg-v $g fixture_ts_output_view owning_node graph", to_string=True)
        _require(_summary("$g"), "AnyPtr{debugger_nested_graph}")
        _require(gdb.execute("hg-p $g nodes 6", to_string=True), "AnyPtr{mesh_}")

        gdb.write("hgraph GDB type-erasure smoke test passed\n")


HGraphSmoke()
