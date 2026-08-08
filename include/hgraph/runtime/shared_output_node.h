#ifndef HGRAPH_RUNTIME_SHARED_OUTPUT_NODE_H
#define HGRAPH_RUNTIME_SHARED_OUTPUT_NODE_H

#include <hgraph/runtime/node.h>

#include <string>
#include <string_view>

namespace hgraph
{
    /** Validate and return the canonical key used to identify a shared output path. */
    [[nodiscard]] HGRAPH_EXPORT std::string output_key(std::string_view path);

    /**
     * Build the pull-source node that owns a shared ``REF<target_schema>`` output.
     *
     * A capture node writes the current target reference into this source node's
     * state and schedules it. The source then publishes its own REF output, so
     * consumers are woken through ordinary output notifications.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_shared_output_source_node(
        std::string path,
        const TSValueTypeMetaData &target_schema,
        bool strict = true);

    /**
     * Build a sink node that captures an input time-series reference and writes
     * it into a paired shared-output source node.
     *
     * Runtime builder example:
     *
     * .. code-block:: cpp
     *
     *    auto path = "svc://prices/to_graph";
     *    gb.add_node(price_source);
     *    gb.add_node(make_shared_output_source_node(path, *ts_price));
     *    gb.add_node(make_shared_output_capture_node(path, *ts_price));
     *    gb.add_edge(GraphEdge{.source_node = 0, .target_node = 2, .target_path = {0}});
     *    gb.add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {1}});
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_shared_output_capture_node(
        std::string path,
        const TSValueTypeMetaData &target_schema);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_SHARED_OUTPUT_NODE_H
