#ifndef HGRAPH_RUNTIME_CONTEXT_NODE_H
#define HGRAPH_RUNTIME_CONTEXT_NODE_H

#include <hgraph/runtime/node.h>

#include <string>
#include <string_view>

namespace hgraph
{
    /**
     * Build the canonical key used for a captured context output.
     *
     * ``scope`` identifies the graph scope that owns the captured output, and
     * ``path`` identifies the context value within that scope. The Python runtime
     * uses keys shaped as ``context-{graph_id}-{path}``; this helper preserves
     * that namespace without prescribing the future C++ wiring syntax.
     */
    [[nodiscard]] HGRAPH_EXPORT std::string context_output_key(std::string_view scope,
                                                               std::string_view path);

    /**
     * Build the pull-source node that owns a captured context ``REF<target_schema>``
     * output.
     *
     * A paired capture node writes the current context reference into this source
     * node's graph-local state and schedules it. Consumers bind to this source
     * output and are woken by ordinary output notifications.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_context_source_node(std::string key,
                                                                     const TSValueTypeMetaData &target_schema);

    /**
     * Build a sink node that captures an input time-series reference and writes
     * it into a paired context source node.
     *
     * Runtime builder example:
     *
     * .. code-block:: cpp
     *
     *    auto key = context_output_key("root", "price");
     *    GraphBuilder gb;
     *    gb.add_node(price_source);
     *    gb.add_node(make_context_source_node(key, *ts_price));
     *    gb.add_node(make_context_capture_node(key, *ts_price));
     *    gb.add_edge(GraphEdge{.source_node = 0, .target_node = 2, .target_path = {0}});
     *    gb.add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {1}});
     *
     * This is an internal graph primitive. User-facing context wiring still
     * needs an approved C++ API.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_context_capture_node(std::string key,
                                                                      const TSValueTypeMetaData &target_schema);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_CONTEXT_NODE_H
