#ifndef HGRAPH_RUNTIME_NODE_CHECKPOINT_H
#define HGRAPH_RUNTIME_NODE_CHECKPOINT_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/node_scheduler_checkpoint.h>
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
    class TSOutputHandle;
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

    /** The identity scopes of a worker-hosted graph (RFC 0039). Every node
     * of one is wired under ``@hgraph.worker``; the runtime's own nodes -- a stage's
     * sources and output sink, and in a ``dmap_`` worker the key partition and
     * the ``map_`` itself -- under ``@hgraph.worker.boundary``, so an image selected by
     * component still takes the input baselines, and the membership that leads
     * to the component, with it. A child template wired from a boundary node
     * is the user's again, and a component inside the graph keeps its own id.
     */
    inline constexpr std::string_view worker_checkpoint_scope{"@hgraph.worker"};
    inline constexpr std::string_view worker_boundary_checkpoint_scope{"@hgraph.worker.boundary"};
    /** Internal scopes cannot be claimed or selected as user components. */
    [[nodiscard]] inline bool reserved_checkpoint_scope(std::string_view component) noexcept
    {
        return component == "@hgraph" || component.starts_with("@hgraph.");
    }

    struct HGRAPH_CLASS_EXPORT NodeCheckpointIdentity
    {
        std::string component{};
        std::string id{};
        std::string signature{};
        /** Why this node cannot be checkpointed, recorded by a worker-graph
         * scope where a component scope would have refused to wire it. */
        std::string refusal{};
        /** A sink with no recordable state (``checkpoint_transient``): inside
         * the scope, outside the image and the contract. It has no id, so it
         * can be added, removed or changed without disturbing anyone else's. */
        bool transient{false};
        /** Producer scopes outside this component, checked against the actual
         * image selection. A parent component's image may include both ends
         * of a dependency that an inner-only image must refuse. */
        std::vector<std::string> input_components{};
    };

    struct HGRAPH_CLASS_EXPORT EndpointBindingCheckpoint
    {
        TSCheckpointLocator binding{};
        TSCheckpointImage clocks{};
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
        std::optional<NodeSchedulerCheckpoint> scheduler{};
        /** Complete active set; omitted static input paths are passive. */
        std::vector<TSInputActivityEntry> input_activity{};
        /** Root-owned synthetic adapter identities and independent historical clocks. */
        std::vector<EndpointBindingCheckpoint> alternatives{};
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
    /** Allocate/import a child recursively without binding references or starting it. */
    using PrepareGraphCheckpoint = RestoreGraphCheckpoint;
    /** Stable owner-local identities for synthetic key/index and publication endpoints. */
    using VisitCheckpointEndpoint = std::function<void(std::size_t, const TSOutputHandle &)>;

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

        inline void start_none(const NodeView &, DateTime) {}
        [[nodiscard]] inline DateTime no_live_schedule(const NodeView &) { return MAX_DT; }
        inline void visit_endpoints_none(const NodeView &, const VisitCheckpointEndpoint &) {}

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
        /** Create saved topology and import child-owned endpoints before REF
         * fixups. Must not start graphs or inspect unresolved input values.
         */
        void (*prepare_restore_impl)(const NodeView &, const NodeCheckpointState &,
                                     DateTime, const PrepareGraphCheckpoint &){&node_checkpoint_detail::restore_none};
        /** Finalize owner/input aliases after all endpoints and REF values
         * exist. The callback finalizes prepared children without starting.
         */
        void (*restore_impl)(const NodeView &, const NodeCheckpointState &,
                             DateTime, const RestoreGraphCheckpoint &){&node_checkpoint_detail::restore_none};
        /** Called after the ordinary owner start, before saved input activity
         * is restored. Starts prepared children in the owner's dependency order.
         */
        void (*start_restored_impl)(const NodeView &, DateTime){&node_checkpoint_detail::start_none};
        /** After the restored start: the earliest time this node still has to
         * run for work that is LIVE rather than historical, or ``MAX_DT``.
         *
         * The coordinator discards a restored node's bootstrap schedule. When
         * an image covers only part of what an owner hosts (RFC 0039, a
         * component inside a worker's child), the rest starts fresh beside
         * it, and a fresh node's start-time schedule is real work the owner
         * has to run. Discarding it would skip scheduled work silently.
         */
        DateTime (*live_schedule_impl)(const NodeView &){&node_checkpoint_detail::no_live_schedule};
        /** Stable ordinals for borrowed synthetic endpoints owned by this node. */
        void (*visit_endpoints_impl)(const NodeView &, const VisitCheckpointEndpoint &){&node_checkpoint_detail::visit_endpoints_none};
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
