"""Read one node at the current LLDB stop. No expressions run in the target."""
import json

import lldb
import hgraph_lldb as hg
import hgraph_lldb_endpoints as endpoints


def at(pointer, index):
    typ = pointer.GetType().GetPointeeType()
    target = pointer.GetTarget()
    return target.CreateValueFromAddress("item", target.ResolveLoadAddress(
        hg.pointer(pointer) + index * typ.GetByteSize()), typ)


def number(value, name):
    return hg.integer(hg.child(value, name))


def path(edge, side):
    depth = number(edge, side + "_depth")
    pointer = hg.child(edge, side + "_path")
    return tuple(hg.integer(at(pointer, i)) for i in range(depth))


def current_node(context):
    thread = context.GetThread()
    for i in range(context.GetFrame().GetFrameID(), thread.GetNumFrames()):
        frame = thread.GetFrameAtIndex(i)
        view = frame.FindVariable("view")
        if not hg.valid(view):
            continue
        if view.GetType().IsReferenceType():
            view = view.Dereference()
        if view.GetType().GetUnqualifiedType().GetName() != "hgraph::NodeView":
            continue
        clock = endpoints.evaluation_time(frame.FindVariable("evaluation_time"))
        if clock is not None:
            return view, clock
    raise ValueError("No evaluating hgraph node in this frame.")


class PausedGraph:
    def __init__(self, view):
        pointer = hg.child(view, "pointer_")
        node_type, node_address, _ = hg.pointer_state(pointer)
        node_record = hg.record_at(view, node_type)
        field = next((f for _, _, f in hg.descriptor_fields(view, endpoints.descriptor(node_record))
                      if f["name"] == "graph"), None)
        if not field:
            raise ValueError("The node has no owning graph.")
        graph = hg.make_indirect_embedded_pointer(view, "graph", node_address + field["offset"])
        graph_type, self.base, _ = hg.pointer_state(graph)
        record = hg.record_at(view, graph_type)
        schema = hg.value_at_address(view, "hgraph::GraphTypeMetaData",
                                     hg.pointer(hg.child(record, "schema")), "schema")
        wiring = hg.child(schema, "debug_wiring")
        desc = endpoints.descriptor(record)
        if number(wiring, "version") != 1 or not hg.common.debug_descriptor_valid(desc):
            raise ValueError("This runtime has no graph inspection metadata.")
        self.nodes = [field for _, _, field in hg.descriptor_fields(view, desc)]
        if len(self.nodes) != number(wiring, "node_count"):
            raise ValueError("Graph layout and wiring disagree.")
        self.ports = hg.child(wiring, "nodes")
        self.root = next((i for i, f in enumerate(self.nodes)
                          if self.base + f["offset"] == node_address and f["type"] == node_type), None)
        if self.root is None:
            raise ValueError("The paused node is outside this graph.")
        self.edges = [[] for _ in self.nodes]
        edges = hg.child(wiring, "edges")
        for i in range(number(wiring, "edge_count")):
            row = at(edges, i)
            source, target = number(row, "source_node"), number(row, "target_node")
            if max(source, target) >= len(self.nodes) or number(row, "source_kind") > 2:
                raise ValueError("Graph connection references an unknown node.")
            edge = source, number(row, "source_kind"), path(row, "source"), target, path(row, "target")
            self.edges[source].append(edge)
            if source != target:
                self.edges[target].append(edge)
        self.names = {}

    def names_for(self, view, i):
        if i not in self.names:
            record = hg.record_at(view, self.nodes[i]["type"])
            schema = hg.value_at_address(view, "hgraph::NodeTypeMetaData",
                                         hg.pointer(hg.child(record, "schema")), "node_schema")
            bundle = hg.child(hg.child(hg.dereference(hg.child(schema, "input_schema")), "data"), "tsb")
            names = [hg.c_string(hg.child(at(hg.child(bundle, "fields"), n), "name"))
                     for n in range(number(bundle, "field_count"))]
            self.names[i] = (hg.c_string(hg.child(schema, "display_name")) or "node", names)
        return self.names[i]

    def describe(self, view, i, clock):
        node = self.nodes[i]
        address = self.base + node["offset"]
        owners = at(self.ports, i)
        absent = (1 << (8 * view.GetTarget().GetAddressByteSize())) - 1

        def storage(owner):
            offsets = hg.child(owners, owner)
            typ = number(offsets, "type_offset")
            if typ == absent:
                return None
            return (hg.read_unsigned(view, address + typ),
                    hg.read_unsigned(view, address + number(offsets, "data_offset")))

        def display(binding):
            if not binding or not binding[0] or binding[1] is None:
                return endpoints.UNAVAILABLE
            return endpoints.display(view, endpoints.resolve_storage(view, *binding), clock)

        def port(name, value):
            return dict(name=name, value=value, links=[])

        label, names = self.names_for(view, i)
        inputs = [port(name, endpoints.UNAVAILABLE) for name in names]
        binding = storage("input")
        if binding and all(binding):
            fields = hg.descriptor_fields(view, endpoints.descriptor(hg.record_at(view, binding[0])))
            for index, _, field in fields:
                if index < len(inputs):
                    inputs[index]["value"] = display((field["type"], binding[1] + field["offset"]))
        outputs = {}
        for kind, name in enumerate(("output", "error_output", "recordable_state")):
            binding = storage(name)
            if binding is not None:
                outputs[kind] = port("out" if kind == 0 else name, display(binding))

        for src, kind, src_path, dst, dst_path in self.edges[i]:
            source_label = self.names_for(view, src)[0]
            target_label, target_names = self.names_for(view, dst)
            output_name = ("out", "error_output", "recordable_state")[kind]
            source_port = output_name + "".join("[key_set]" if n == absent else f"[{n}]" for n in src_path)
            slot = dst_path[0] if dst_path else None
            target_port = target_names[slot] if slot is not None and slot < len(target_names) else "input"
            target_port += "".join(f"[{n}]" for n in dst_path[1:])
            if src == i and kind in outputs:
                outputs[kind]["links"].append(dict(node=dst, label=target_label, port=target_port, via=source_port))
            if dst == i:
                if slot is not None and slot < len(names):
                    destination = inputs[slot]
                else:
                    if len(inputs) == len(names):
                        inputs.append(port("input", endpoints.UNAVAILABLE))
                    destination = inputs[-1]
                destination["links"].append(dict(node=src, label=source_label, port=source_port, via=target_port))
        return dict(node=i, label=label, inputs=inputs, outputs=list(outputs.values()))


# One stopped graph per adapter. Drop all cached SBValues when the stop changes.
_cached_key = None
_cached_graph = None
_snapshot = 0


def inspect_graph(debugger, command, context, result, _internal):
    global _cached_key, _cached_graph, _snapshot
    report = {"version": 1}
    try:
        process = context.GetProcess()
        if process.GetState() != lldb.eStateStopped:
            raise ValueError("Pause at a node to inspect the graph.")
        stop = process.GetStopID()
        view, clock = current_node(context)
        key = process.GetUniqueID(), stop, hg.pointer_state(hg.child(view, "pointer_"))[:2]
        if key != _cached_key:
            _cached_graph = PausedGraph(view)
            _cached_key = key
            _snapshot += 1
        args = command.split()
        if not args:
            index = _cached_graph.root
        elif len(args) == 2 and int(args[0]) == _snapshot and 0 <= int(args[1]) < len(_cached_graph.nodes):
            index = int(args[1])
        else:
            raise ValueError("The selected node does not belong to this stop.")
        report.update(stop=stop, snapshot=_snapshot, **_cached_graph.describe(view, index, clock))
    except (ValueError, IndexError, TypeError, RuntimeError) as error:
        report["error"] = str(error)
    result.AppendMessage(json.dumps(report, ensure_ascii=True))


def __lldb_init_module(debugger, _internal):
    debugger.HandleCommand("command script add -f hgraph_lldb_graph.inspect_graph hgraph-graph")
