import typing
from collections import defaultdict, deque

from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_signature import WiringNodeType
from hgraph._wiring._wiring_port import WiringPort

if typing.TYPE_CHECKING:
    from hgraph._builder._graph_builder import GraphBuilder, GraphBuilderFactory
    from hgraph._wiring._wiring_node_instance import WiringNodeInstance

__all__ = ("wire_graph", "create_graph_builder")


def wire_graph(graph, *args, **kwargs) -> "GraphBuilder":
    """
    Evaluate the wiring graph and build a runtime graph.
    This graph is the actual graph objects that are used to be evaluated.
    """
    from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext

    from hgraph._builder._ts_builder import TimeSeriesBuilderFactory
    from hgraph._wiring._wiring_errors import WiringError

    if not TimeSeriesBuilderFactory.has_instance():
        TimeSeriesBuilderFactory.declare_default_factory()

    try:
        with WiringGraphContext(None) as context:
            out = graph(*args, **kwargs)
            # For now let's ensure that top level graphs do not return anything.
            # Later we can consider default behaviour for graphs with outputs.
            assert out is None, "Currently only graph with no return values are supported"

            context.build_services()

            # Build graph by walking from sink nodes to parent nodes.
            # Also eliminate duplicate nodes
            sink_nodes = context.sink_nodes
            return create_graph_builder(sink_nodes)
    except WiringError as e:
        e.print_error()
        raise e


EDGE_TYPE = None


def create_graph_builder(sink_nodes: tuple["WiringNodeInstance"], supports_push_nodes: bool = True) -> "GraphBuilder":
    """
    Create a graph builder instance. This is called with the sink_nodes created during the wiring of a graph.
    This is extracted to support nested graph construction, where the sink nodes are limited to the new nested graph,
    but we wish to keep the nesting to allow for better debug information to be accumulated.
    """
    from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeInstance
    from hgraph._builder._graph_builder import Edge
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._builder._graph_builder import GraphBuilderFactory

    if not sink_nodes:
        raise RuntimeError("No sink nodes found in graph")

    ranked_nodes = toposort(sink_nodes, supports_push_nodes)

    # Now we can walk the tree in rank order and construct the nodes
    node_map: dict[WiringNodeInstance, int] = {}
    node_builders: [NodeBuilder] = []
    edges: set[Edge] = set[Edge]()
    for wiring_node in ranked_nodes:
        if wiring_node.is_stub:
            continue
        ndx = len(node_builders)
        node_builder, input_edges = wiring_node.create_node_builder_and_edges(node_map, node_builders)
        for edge in input_edges:
            if edge.src_node >= edge.dst_node:
                raise RuntimeError(
                    f"Cycle detected at node: {wiring_node.resolved_signature.signature} on input:"
                    f" {wiring_node.resolved_signature.args[edge.input_path[0]]}"
                )
        node_builders.append(node_builder)
        edges.update(input_edges)
        node_map[wiring_node] = ndx

    return GraphBuilderFactory.make(
        node_builders=tuple[NodeBuilder, ...](node_builders),
        edges=tuple[Edge, ...](sorted(edges, key=lambda e: (e.src_node, e.dst_node, e.output_path, e.input_path))),
    )


def toposort(
    nodes: typing.Sequence["WiringNodeInstance"], supports_push_nodes: bool = True
) -> typing.Sequence["WiringNodeInstance"]:
    mapping: dict["WiringNodeInstance", set["WiringNodeInstance"]] = defaultdict(set)
    reverse_mapping: dict["WiringNodeInstance", set["WiringNodeInstance"]] = defaultdict(set)
    nodes_to_process: deque["WiringNodeInstance"] = deque(nodes)
    source_nodes = set()
    processed_nodes = dict["WiringNodeInstance", int]()
    # Build node adjacency matrix and collect source nodes.
    while len(nodes_to_process) > 0:
        to_node = nodes_to_process.popleft()
        if to_node in processed_nodes:  # This could be done better
            continue  # This node has already been processed
        else:
            processed_nodes[to_node] = 1
        ts_nodes = [n.node_instance for n in to_node.inputs.values() if isinstance(n, WiringPort)]
        ts_nodes.extend(n for n in to_node.non_input_dependencies)
        for from_node in ts_nodes:
            for alt_node in from_node.ranking_alternatives:
                mapping[from_node].add(alt_node)
                reverse_mapping[alt_node].add(from_node)
                nodes_to_process.append(alt_node)
            mapping[from_node].add(to_node)
            reverse_mapping[to_node].add(from_node)
            nodes_to_process.append(from_node)
        if not ts_nodes:
            source_nodes.add(to_node)

    from graphlib import TopologicalSorter, CycleError

    try:
        ordered = list(TopologicalSorter(reverse_mapping).static_order())
    except CycleError as e:
        cycle_nodes = e.args[1]

        cycle_node_strings = []
        for n in cycle_nodes:
            scalars = ", ".join(
                f"{k}={str(v)[:100]}" for k, v in n.inputs.items() if k in n.resolved_signature.scalar_inputs
            )
            cycle_node_strings.append(f"{n.wiring_path_name}.{n.label or n.node_signature.name}({scalars})")

        cycle_print = "\n -> ".join(cycle_node_strings)

        raise RuntimeError(f"Cyclic sub graph detected that involves nodes: \n{cycle_print}")

    push_nodes = [n for n in ordered if n.resolved_signature.node_type is WiringNodeType.PUSH_SOURCE_NODE]
    other_nodes = [
        n for n in ordered if not n.resolved_signature.node_type is WiringNodeType.PUSH_SOURCE_NODE and not n.is_stub
    ]

    if len(push_nodes) > 0 and not supports_push_nodes:
        raise CustomMessageWiringError(
            f"Graph contains push nodes: {push_nodes} but this graph does not support push nodes."
        )

    return push_nodes + other_nodes
