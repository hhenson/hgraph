#ifndef HGRAPH_RUNTIME_NODE_ERROR_H
#define HGRAPH_RUNTIME_NODE_ERROR_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <optional>
#include <string>

namespace hgraph
{
    struct ValueTypeMetaData;
    struct TSValueTypeMetaData;
    class NodeView;

    /** Diagnostic detail requested when a node or child graph captures an error. */
    struct HGRAPH_EXPORT ErrorCaptureOptions
    {
        std::size_t trace_back_depth{1};
        bool        capture_values{false};

        [[nodiscard]] bool operator==(const ErrorCaptureOptions &) const noexcept = default;
    };

    /**
     * ``NodeError`` — the value reported on a node's error output time series
     * when an evaluation throws (Python's ``NodeError`` ``CompoundScalar``).
     *
     * It is a value-layer **compound scalar**: a named ``bundle`` ("NodeError")
     * of ``str`` fields. This marker type names it so ``TS<NodeError>`` resolves
     * statically (``scalar_descriptor<NodeError>`` below); the value itself is a
     * bundle whose fields mirror the reference (see ``error_handling.rst``).
     */
    struct NodeError
    {
    };

    /** The interned value-layer meta for ``NodeError`` (the named bundle). */
    [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *node_error_value_meta();

    /** The interned ``TS<NodeError>`` meta. */
    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *node_error_ts_meta();

    /** The structurally-meaningful fields captured when building a ``NodeError``. */
    struct HGRAPH_EXPORT NodeErrorFields
    {
        std::string signature_name{};
        std::string label{};
        std::string wiring_path{};
        std::string error_msg{};
        std::string stack_trace{};
        std::string activation_back_trace{};
        std::optional<std::string> additional_context{};
    };

    /** Capture node identity and a bounded active-input trace on the error path. */
    [[nodiscard]] HGRAPH_EXPORT NodeErrorFields capture_node_error(
        const NodeView &node,
        DateTime evaluation_time,
        std::string error_msg,
        ErrorCaptureOptions options = {},
        std::optional<std::string> additional_context = {});

    /** Build an owning ``NodeError`` bundle ``Value`` from ``fields``. */
    [[nodiscard]] HGRAPH_EXPORT Value make_node_error_value(const NodeErrorFields &fields);

    template <>
    struct scalar_descriptor<NodeError>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return true; }

        [[nodiscard]] static const ValueTypeMetaData *value_meta() { return node_error_value_meta(); }
    };
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NODE_ERROR_H
