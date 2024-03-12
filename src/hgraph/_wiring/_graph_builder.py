import typing
from collections import defaultdict

from ordered_set import OrderedSet

from hgraph._wiring._wiring_errors import CustomMessageWiringError

if typing.TYPE_CHECKING:
    from hgraph._builder._graph_builder import GraphBuilder, GraphBuilderFactory
    from hgraph._wiring._wiring_node_instance import WiringNodeInstance, WiringNodeInstanceContext

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


def create_graph_builder(sink_nodes: tuple["WiringNodeInstance"], supports_push_nodes: bool = True) -> "GraphBuilder":
    """
    Create a graph builder instance. This is called with the sink_nodes created during the wiring of a graph.
    This is extracted to support nested graph construction, where the sink nodes are limited to the new nested graph,
    but we wish to keep the nesting to allow for better debug information to be accumulated.
    """
    from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeInstance
    from hgraph._builder._graph_builder import Edge
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._runtime._node import NodeTypeEnum
    from hgraph._builder._graph_builder import GraphBuilderFactory

    if not sink_nodes:
        raise RuntimeError("No sink nodes found in graph")

    max_rank = max(node.rank for node in sink_nodes)
    ranked_nodes: dict[int, OrderedSet[WiringNodeInstance]] = defaultdict(OrderedSet)

    processed_nodes = OrderedSet(sink_nodes)
    pending_nodes = list(processed_nodes)
    while pending_nodes:
        node = pending_nodes.pop()
        if (rank := node.rank) == 1:
            # Put all push nodes at rank 0 and pull nodes at rank 1
            rank = 0 if node.resolved_signature.node_type is NodeTypeEnum.PUSH_SOURCE_NODE else 1
            if not supports_push_nodes and rank == 0:
                raise CustomMessageWiringError(
                    f'Node: {node.resolved_signature} is a pull source node, '
                    f'but this graph does not support push nodes.')
        if node.resolved_signature.node_type is NodeTypeEnum.SINK_NODE:
            # Put all sink nodes at max_rank
            rank = max_rank
        ranked_nodes[rank].add(node)
        for arg in filter(lambda k_: k_ in node.resolved_signature.time_series_inputs,
                          node.resolved_signature.args):
            if (input_ := node.inputs.get(arg)) and (next_node := input_.node_instance) not in processed_nodes:
                processed_nodes.add(next_node)
                pending_nodes.append(next_node)

    # Now we can walk the tree in rank order and construct the nodes
    node_map: dict[WiringNodeInstance, int] = {}
    node_builders: [NodeBuilder] = []
    edges: set[Edge] = set[Edge]()
    for i in range(max_rank + 1):
        wiring_node_set = ranked_nodes.get(i, set())
        for wiring_node in wiring_node_set:
            if wiring_node.is_stub:
                continue
            ndx = len(node_builders)
            node_builder, input_edges = wiring_node.create_node_builder_and_edges(node_map, node_builders)
            node_builders.append(node_builder)
            edges.update(input_edges)
            node_map[wiring_node] = ndx

    return GraphBuilderFactory.make(node_builders=tuple[NodeBuilder, ...](node_builders),
                                    edges=tuple[Edge, ...](
                                        sorted(edges,
                                               key=lambda e: (
                                                   e.src_node, e.dst_node, e.output_path, e.input_path))))
