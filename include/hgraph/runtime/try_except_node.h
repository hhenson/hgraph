#ifndef HGRAPH_RUNTIME_TRY_EXCEPT_NODE_H
#define HGRAPH_RUNTIME_TRY_EXCEPT_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/nested_graph_node.h>

namespace hgraph
{
    /**
     * Build a **try/except node**: a single-child-graph node (the
     * ``single_nested_graph_node`` substrate) whose evaluation runs the child
     * graph under a try/catch. On a normal cycle the child writes through to the
     * output's ``out`` field (forwarded via ``spec.output_binding`` with a
     * ``target_path`` into the bundle); on an exception the node writes a
     * ``NodeError`` to the ``exception`` field instead and the graph continues.
     *
     * ``meta.output_schema`` is either a ``TSB`` with fields ``exception``
     * (``TS<NodeError>``) and ``out`` (the wrapped graph output) — for which
     * ``spec.output_binding->target_path`` names the ``out`` field — or, for a
     * **sink** sub-graph, a bare ``TS<NodeError>`` with no ``output_binding``.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder try_except_node(
        NodeTypeMetaData meta,
        SingleNestedGraphNodeSpec spec,
        SingleNestedGraphNodeOptions options = {},
        ErrorCaptureOptions error_capture = {});
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_TRY_EXCEPT_NODE_H
