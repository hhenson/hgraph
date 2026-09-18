#ifndef HGRAPH_RUNTIME_GRAPH_CHECKPOINT_COORDINATOR_H
#define HGRAPH_RUNTIME_GRAPH_CHECKPOINT_COORDINATOR_H

#include <hgraph/runtime/lifecycle_observer.h>
#include <hgraph/runtime/node_checkpoint.h>

#include <memory>
#include <string>
#include <string_view>

namespace hgraph
{
    /** Which nodes of a graph one image covers (RFC 0039, "Whole-graph coordinator"). */
    class HGRAPH_CLASS_EXPORT GraphCheckpointSelection
    {
      public:
        /** Every node: a worker-hosted graph is its own recovery boundary. */
        [[nodiscard]] static GraphCheckpointSelection whole_graph();
        /** The nodes ``component``, or a component nested inside it, owns. */
        [[nodiscard]] static GraphCheckpointSelection owned_by(std::string component);

        [[nodiscard]] bool whole() const noexcept { return component_.empty(); }
        /** ``owner`` is a node's ``NodeCheckpointIdentity::component``. */
        [[nodiscard]] bool selects(std::string_view owner) const noexcept;

      private:
        std::string component_{};
    };

    /** The graph-image mechanics of RFC 0023: capture, static validation, the
     * prepare / fix-up / finalise restore and reference locators.
     *
     * It carries no policy. ``ComponentRecoverySession`` is its completed-day
     * client; an externally driven executor is its whole-graph client
     * (``start_external_restored`` / ``capture_external``). Every operation is
     * a cold lifecycle path.
     *
     * It is the ``LifecycleObserver`` that starts prepared children and
     * restores saved input activity after each node's start hook, so a client
     * registers it for the start phase that follows ``restore``.
     */
    class HGRAPH_CLASS_EXPORT GraphCheckpointCoordinator final : public LifecycleObserver
    {
      public:
        explicit GraphCheckpointCoordinator(GraphCheckpointSelection selection);
        ~GraphCheckpointCoordinator() override;
        GraphCheckpointCoordinator(const GraphCheckpointCoordinator &) = delete;
        GraphCheckpointCoordinator &operator=(const GraphCheckpointCoordinator &) = delete;

        /** Identities and contract signatures only: what an image must match. */
        [[nodiscard]] GraphCheckpointImage shape(const GraphView &graph);
        /** The owned image at ``graph.evaluation_time()``, a completed cycle. */
        [[nodiscard]] GraphCheckpointImage capture(const GraphView &graph);
        /** Import ``image`` into the unstarted ``graph``. Every restored
         * timestamp must be at or before ``cut``. A failure detaches the whole
         * preparation before it propagates. ``image`` is borrowed and must
         * outlive the start phase.
         */
        void restore(const GraphView &graph, const GraphCheckpointImage &image,
                     DateTime start, DateTime cut);
        /** Release preparation cleanup only after the complete root start succeeds. */
        void complete_start() noexcept;

        /** What a client records to refuse an image of a different graph or revision. */
        [[nodiscard]] static std::string signature(const GraphCheckpointImage &image,
                                                   std::string_view revision);

        void on_after_start_node(const NodeView &node) override;
        void on_start_node_failed(const NodeView &node) override;
        void on_start_graph_failed(const GraphView &graph) override;

      private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };
}
#endif
