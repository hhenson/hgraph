from collections import defaultdict

from hg._runtime import Graph, NodeTypeEnum, Node


def build_runtime_graph(graph, *args, **kwargs) -> Graph:
    """
    Evaluate the wiring graph and build a runtime graph.
    This graph is the actual graph objects that are used to be evaluated.
    """
    from hg import WiringGraphContext
    from hg._wiring._wiring import WiringNodeInstance

    with WiringGraphContext(None) as context:
        out = graph(*args, **kwargs)
        # For now let's ensure that top level graphs do not return anything.
        # Later we can consider default behaviour for graphs with outputs.
        assert out is None, "Currently only graph with no return values are supported"

        # Build graph by walking from sink nodes to parent nodes.
        # Also eliminate duplicate nodes
        sink_nodes = context.sink_nodes
        max_rank = max(node.rank for node in sink_nodes)
        ranked_nodes: dict[int, set[WiringNodeInstance]] = defaultdict(set)

        pending_nodes = list(sink_nodes)
        while pending_nodes:
            node = pending_nodes.pop()
            if (rank := node.rank) == 1:
                # Put all push nodes at rank 0 and pull nodes at rank 1
                rank = 0 if node.resolved_signature.node_type is NodeTypeEnum.PUSH_SOURCE_NODE else 1
            if node.resolved_signature.node_type is NodeTypeEnum.SINK_NODE:
                # Put all sink nodes at max_rank
                rank = max_rank
            ranked_nodes[node.rank].add(node)
            for arg in node.resolved_signature.time_series_args:
                pending_nodes.append(node.inputs[arg].node_instance)

        # Now we can walk the tree in rank order and construct the nodes
        node_map: dict[WiringNodeInstance, int] = {}
        nodes: [Node] = []
        for i in range(max_rank+1):
            wiring_node_set = ranked_nodes.get(i, set())
            for wiring_node in wiring_node_set:
                ndx = len(nodes)
                node = wiring_node.create_node_instance(node_map, nodes)
                nodes.append(node)
                node_map[wiring_node] = ndx

    return Graph(tuple(nodes))




