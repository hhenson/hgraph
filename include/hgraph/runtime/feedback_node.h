#ifndef HGRAPH_RUNTIME_FEEDBACK_NODE_H
#define HGRAPH_RUNTIME_FEEDBACK_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/node.h>

namespace hgraph
{
    struct TSValueTypeMetaData;

    /**
     * Feedback source: a pull source whose state holds one canonical TS delta
     * value (``output_schema.delta_value_schema``). When scheduled it replays
     * that delta to its output.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_feedback_source_node(
        const TSValueTypeMetaData &output_schema,
        bool has_initial_delta = false);

    /**
     * Feedback sink: active on ``ts`` and passive on ``ts_self``. Each tick copies
     * ``capture_delta(ts)`` into the owner node of ``ts_self`` and schedules that
     * source node for the next engine cycle.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_feedback_sink_node(
        const TSValueTypeMetaData &schema);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_FEEDBACK_NODE_H
