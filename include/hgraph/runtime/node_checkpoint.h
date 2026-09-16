#ifndef HGRAPH_RUNTIME_NODE_CHECKPOINT_H
#define HGRAPH_RUNTIME_NODE_CHECKPOINT_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/time_series/ts_data/checkpoint.h>
#include <hgraph/types/time_series/ts_input/activity.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace hgraph
{
    class GraphView;
    class NodeView;
    class NodeBuilder;
    struct GraphCheckpointImage;

    /** A live runtime child identified by its owner's stable slot and key.
     * Images own their values and never retain runtime graph addresses.
     */
    struct HGRAPH_CLASS_EXPORT ChildGraphCheckpoint
    {
        std::size_t slot{0};
        Value key{};
        DateTime key_last_modified_time{MIN_DT};
        std::shared_ptr<GraphCheckpointImage> graph{};
    };

    /** Representation-owned semantic state, beside the ordinary endpoints. */
    struct HGRAPH_CLASS_EXPORT NodeCheckpointState
    {
        Value payload{};
        /** Hidden owned endpoints or clock-only aliases interpreted by this owner. */
        std::vector<TSCheckpointImage> endpoints{};
        std::vector<ChildGraphCheckpoint> children{};
    };

    struct HGRAPH_CLASS_EXPORT NodeCheckpointIdentity
    {
        std::string component{};
        std::string id{};
        std::string signature{};
    };

    struct HGRAPH_CLASS_EXPORT NodeCheckpointImage
    {
        std::string id{};
        std::string signature{};
        std::optional<TSCheckpointImage> output{};
        std::optional<TSCheckpointImage> error{};
        std::optional<TSCheckpointImage> recordable_state{};
        /** Direct external source baseline admitted through a component input. */
        std::optional<TSCheckpointImage> ingress{};
        /** Complete active set; omitted static input paths are passive. */
        std::vector<TSInputActivityEntry> input_activity{};
        NodeCheckpointState custom{};
    };

    struct HGRAPH_CLASS_EXPORT GraphCheckpointImage
    {
        std::vector<NodeCheckpointImage> nodes{};
    };

    using CaptureGraphCheckpoint =
        std::function<std::shared_ptr<GraphCheckpointImage>(const GraphView &)>;
    using RestoreGraphCheckpoint =
        std::function<void(const GraphView &, const GraphCheckpointImage &, DateTime)>;

    namespace node_checkpoint_detail
    {
        [[nodiscard]] inline NodeCheckpointState capture_none(
            const NodeView &, const CaptureGraphCheckpoint &)
        {
            return {};
        }

        inline void restore_none(const NodeView &, const NodeCheckpointState &,
                                 DateTime, const RestoreGraphCheckpoint &)
        {}

        inline std::string signature_none(const NodeBuilder &) { return {}; }
    }

    /** Cold-path contract installed by a concrete node representation.
     *
     * The graph coordinator owns ordinary endpoint capture and recursive
     * graph restore. Dynamic owners expose and reconstruct their children
     * through this contract without exposing private runtime containers.
     * Absent support is explicit; operation pointers remain non-null.
     */
    struct HGRAPH_CLASS_EXPORT NodeCheckpointOps
    {
        bool supported{false};
        bool captures_output{true};
        /** This node owns a component input boundary, including source baseline. */
        bool boundary_input{false};
        NodeCheckpointState (*capture_impl)(
            const NodeView &, const CaptureGraphCheckpoint &){&node_checkpoint_detail::capture_none};
        void (*restore_impl)(const NodeView &, const NodeCheckpointState &,
                             DateTime, const RestoreGraphCheckpoint &){&node_checkpoint_detail::restore_none};
        std::string (*signature_impl)(const NodeBuilder &){&node_checkpoint_detail::signature_none};
        std::string (*id_impl)(const NodeBuilder &){&node_checkpoint_detail::signature_none};
    };

    [[nodiscard]] inline const NodeCheckpointOps &unsupported_node_checkpoint_ops() noexcept
    {
        static const NodeCheckpointOps ops{};
        return ops;
    }
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NODE_CHECKPOINT_H
