from hgraph import compute_node, TIME_SERIES_TYPE, SCALAR, TSD, REF, TS, TS_OUT, STATE, TimeSeriesDictOutput, SCHEDULER, \
    MIN_TD

__all__ = ("mesh_subscribe_node",)


@compute_node
def mesh_subscribe_node(mesh: REF[TSD[SCALAR, TIME_SERIES_TYPE]], item: TS[SCALAR],
                        _output: TS_OUT[TIME_SERIES_TYPE] = None,
                        state: STATE = None,
                        scheduler: SCHEDULER = None
                        ) -> REF[TSD[SCALAR, TIME_SERIES_TYPE]]:
    """
    Subscribes to the mesh with the given item
    """
    if scheduler.is_scheduled and not item.modified:
        return mesh.value

    from hgraph._impl._runtime._mesh_node import PythonMeshNodeImpl

    mesh_node: PythonMeshNodeImpl = mesh.value.output.owning_node
    key = _find_mesh_key(_output.owning_node, mesh_node)

    if _output.valid:
        mesh_node._remove_graph_dependency(key, state.key)

    new_dependency_key = item.value
    available_now = mesh_node._add_graph_dependency(key, new_dependency_key)
    state.key = item.value

    if available_now:
        return mesh.value
    else:
        scheduler.schedule(MIN_TD)


def _find_mesh_key(self_node, mesh_node):
    node = self_node
    while (clock := node.graph.engine_evaluation_clock) and clock.node != mesh_node:
        node = clock.node
    key = clock.key  # this is the key this graph belongs to in the mesh
    return key


@mesh_subscribe_node.stop
def mesh_subscribe_node_stop(mesh: REF[TSD[SCALAR, TIME_SERIES_TYPE]],
                             _output: TS_OUT[TIME_SERIES_TYPE], state: STATE):
    from hgraph._impl._runtime._mesh_node import PythonMeshNodeImpl

    mesh_node: PythonMeshNodeImpl = mesh.value.output.owning_node

    if _output.valid:
        mesh_node._remove_graph_dependency(_find_mesh_key(_output.owning_node, mesh_node), state.key)
