"""Read one node at the current LLDB stop. No expressions run in the target."""
import json

import lldb
import hgraph_lldb as hg
import hgraph_lldb_endpoints as endpoints

# Every count read from the target is checked against a limit before a loop
# uses it, so a corrupt or foreign count is reported instead of stalling LLDB.
MAX_NODES = 1 << 16
MAX_CONNECTIONS = 1 << 18
MAX_FIELDS = 1 << 12
MAX_PATH_DEPTH = 64

TIME_SERIES = 2  # TypeFamily::TimeSeries
TSB = 5  # TSTypeKind::TSB


def bounded(count, limit, what):
    if count > limit:
        raise ValueError(f"{what} {count} exceeds the limit of {limit}.")
    return count


def at(pointer, index):
    typ = pointer.GetType().GetPointeeType()
    target = pointer.GetTarget()
    return target.CreateValueFromAddress("item", target.ResolveLoadAddress(
        hg.pointer(pointer) + index * typ.GetByteSize()), typ)


def number(value, name):
    return hg.integer(hg.child(value, name))


def path(edge, side):
    depth = bounded(number(edge, side + "_depth"), MAX_PATH_DEPTH, "Connection path depth")
    pointer = hg.child(edge, side + "_path")
    return tuple(hg.integer(at(pointer, i)) for i in range(depth))


def fields(view, desc, limit, what):
    """Plain field snapshots of a valid debug descriptor."""
    if not hg.common.debug_descriptor_valid(desc):
        return []
    bounded(desc["field_count"], limit, what)
    return [field for _, _, field in hg.descriptor_fields(view, desc)]


def input_names(schema):
    """Field names of a bundle input. An input of another kind has none."""
    header = hg.schema_snapshot(hg.child(schema, "header"))
    if (header["magic"], header["abi_version"], header["family"], number(schema, "kind")) != (
            hg.common.SCHEMA_HEADER_MAGIC, hg.common.SCHEMA_HEADER_ABI_VERSION, TIME_SERIES, TSB):
        return []
    bundle = hg.child(hg.child(schema, "data"), "tsb")
    count = bounded(number(bundle, "field_count"), MAX_FIELDS, "Input field count")
    names = hg.child(bundle, "fields")
    return [hg.c_string(hg.child(at(names, n), "name")) for n in range(count)]


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


def wiring_header(view, record):
    """The published wiring of a graph type record, and the words that identify it."""
    schema_address = hg.pointer(hg.child(record, "schema"))
    schema = hg.value_at_address(view, "hgraph::GraphTypeMetaData", schema_address, "schema")
    wiring = hg.child(schema, "debug_wiring")
    identity = (schema_address,) + tuple(
        number(wiring, name) for name in ("version", "nodes", "node_count", "edges", "edge_count"))
    return wiring, identity


class GraphWiring:
    """Nodes, port offsets, connections and input names of one graph type.

    The registry publishes these once and never changes them, so they are read
    once and kept as plain values across stops.
    """

    def __init__(self, view, record, wiring, identity):
        self.identity = identity
        version = number(wiring, "version")
        desc = endpoints.descriptor(record)
        if version == 0 or not hg.common.debug_descriptor_valid(desc):
            raise ValueError("This runtime has no graph inspection metadata.")
        if version != 1:
            raise ValueError(f"Graph inspection metadata version {version} is not supported.")
        self.nodes = fields(view, desc, MAX_NODES, "Graph node count")
        if len(self.nodes) != number(wiring, "node_count"):
            raise ValueError("Graph layout and wiring disagree.")
        self.absent = (1 << (8 * view.GetTarget().GetAddressByteSize())) - 1
        ports = hg.child(wiring, "nodes")
        self.ports = [self.read_ports(at(ports, i)) for i in range(len(self.nodes))]
        self.edges = [[] for _ in self.nodes]
        edges = hg.child(wiring, "edges")
        for i in range(bounded(number(wiring, "edge_count"), MAX_CONNECTIONS, "Connection count")):
            row = at(edges, i)
            source, target = number(row, "source_node"), number(row, "target_node")
            if max(source, target) >= len(self.nodes) or number(row, "source_kind") > 2:
                raise ValueError("Graph connection references an unknown node.")
            edge = source, number(row, "source_kind"), path(row, "source"), target, path(row, "target")
            self.edges[source].append(edge)
            if source != target:
                self.edges[target].append(edge)
        self._names = {}

    def read_ports(self, row):
        ports = {}
        for owner in ("input", "output", "error_output", "recordable_state"):
            offsets = hg.child(row, owner)
            typ = number(offsets, "type_offset")
            ports[owner] = None if typ == self.absent else (typ, number(offsets, "data_offset"))
        return ports

    def names(self, view, i):
        if i not in self._names:
            record = hg.record_at(view, self.nodes[i]["type"])
            schema = hg.value_at_address(view, "hgraph::NodeTypeMetaData",
                                         hg.pointer(hg.child(record, "schema")), "node_schema")
            self._names[i] = (hg.c_string(hg.child(schema, "display_name")) or "node",
                              input_names(hg.dereference(hg.child(schema, "input_schema"))))
        return self._names[i]


# Wiring of each graph type seen in the current process.
_wiring = {}
_wiring_process = None


def wiring_for(view, process_id, graph_type):
    global _wiring_process
    if process_id != _wiring_process:
        _wiring.clear()
        _wiring_process = process_id
    record = hg.record_at(view, graph_type)
    wiring, identity = wiring_header(view, record)
    cached = _wiring.get(graph_type)
    # A test-only registry reset can reuse a record address for other wiring.
    if cached is None or cached.identity != identity:
        cached = _wiring[graph_type] = GraphWiring(view, record, wiring, identity)
    return cached


class PausedGraph:
    """The graph instance that owns the paused node, read again at each stop."""

    def __init__(self, view, process_id):
        node_type, node_address, _ = hg.pointer_state(hg.child(view, "pointer_"))
        node_record = hg.record_at(view, node_type)
        field = next((f for f in fields(view, endpoints.descriptor(node_record), MAX_FIELDS, "Node field count")
                      if f["name"] == "graph"), None)
        if not field:
            raise ValueError("The node has no owning graph.")
        graph = hg.make_indirect_embedded_pointer(view, "graph", node_address + field["offset"])
        self.graph_type, self.base, _ = hg.pointer_state(graph)
        self.wiring = wiring_for(view, process_id, self.graph_type)
        self.nodes = self.wiring.nodes
        self.root = next((i for i, f in enumerate(self.nodes)
                          if self.base + f["offset"] == node_address and f["type"] == node_type), None)
        if self.root is None:
            raise ValueError("The paused node is outside this graph.")

    def describe(self, view, i, clock):
        wiring = self.wiring
        address = self.base + self.nodes[i]["offset"]
        absent = wiring.absent

        def storage(owner):
            offsets = wiring.ports[i][owner]
            if offsets is None:
                return None
            return (hg.read_unsigned(view, address + offsets[0]),
                    hg.read_unsigned(view, address + offsets[1]))

        def display(binding):
            if not binding or not binding[0] or binding[1] is None:
                return endpoints.UNAVAILABLE
            return endpoints.display(view, endpoints.resolve_storage(view, *binding), clock)

        def port(name, value):
            return dict(name=name, value=value, links=[])

        label, names = wiring.names(view, i)
        inputs = [port(name, endpoints.UNAVAILABLE) for name in names]
        binding = storage("input")
        if binding and all(binding):
            desc = endpoints.descriptor(hg.record_at(view, binding[0]))
            for index, field in enumerate(fields(view, desc, MAX_FIELDS, "Input field count")):
                if index < len(inputs):
                    inputs[index]["value"] = display((field["type"], binding[1] + field["offset"]))
        outputs = {}
        for kind, name in enumerate(("output", "error_output", "recordable_state")):
            binding = storage(name)
            if binding is not None:
                outputs[kind] = port("out" if kind == 0 else name, display(binding))

        for src, kind, src_path, dst, dst_path in wiring.edges[i]:
            source_label = wiring.names(view, src)[0]
            target_label, target_names = wiring.names(view, dst)
            output_name = ("out", "error_output", "recordable_state")[kind]
            source_port = output_name + "".join("[key_set]" if n == absent else f"[{n}]" for n in src_path)
            slot = dst_path[0] if dst_path else None
            if slot is not None and slot < len(target_names):
                target_port = target_names[slot] + "".join(f"[{n}]" for n in dst_path[1:])
            else:
                target_port = "input" + "".join(f"[{n}]" for n in dst_path)
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


# A snapshot names one graph instance at one stop; node numbers are local to it.
_snapshot_key = None
_snapshot = 0


def inspect_graph(debugger, command, context, result, _internal):
    global _snapshot_key, _snapshot
    report = {"version": 1}
    try:
        process = context.GetProcess()
        if process.GetState() != lldb.eStateStopped:
            raise ValueError("Pause at a node to inspect the graph.")
        stop = process.GetStopID()
        view, clock = current_node(context)
        graph = PausedGraph(view, process.GetUniqueID())
        key = process.GetUniqueID(), stop, graph.graph_type, graph.base
        if key != _snapshot_key:
            _snapshot_key = key
            _snapshot += 1
        args = command.split()
        if not args:
            index = graph.root
        elif len(args) == 2 and int(args[0]) == _snapshot and 0 <= int(args[1]) < len(graph.nodes):
            index = int(args[1])
        else:
            raise ValueError("The selected node does not belong to this stop.")
        report.update(stop=stop, snapshot=_snapshot, **graph.describe(view, index, clock))
    except (ValueError, IndexError, TypeError, RuntimeError) as error:
        report["error"] = str(error)
    result.AppendMessage(json.dumps(report, ensure_ascii=True))


def __lldb_init_module(debugger, _internal):
    debugger.HandleCommand("command script add -f hgraph_lldb_graph.inspect_graph hgraph-graph")
