#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/v2/evaluation_engine.h>
#include <hgraph/types/v2/node.h>

#include <cstddef>
#include <memory>
#include <span>

namespace hgraph::v2
{
    /** Schedule entry pairing the next requested evaluation time with a node. */
    struct HGRAPH_EXPORT NodeEntry
    {
        engine_time_t scheduled{MIN_DT};
        Node *node{nullptr};
    };

    /** Prevalidated push-source node reference used by the realtime push pass. */
    struct HGRAPH_EXPORT PushSourceNodeRef
    {
        Node &node;
        const PushSourceNodeRuntimeOps &runtime_ops;
    };

    /**
     * Runtime graph owning the v2 node slab.
     *
     * Graph owns one contiguous storage block containing NodeEntry[N] followed
     * by the variable-sized node chunks created by GraphBuilder. Evaluation is
     * driven through an attached GraphEvaluationEngine supplied by the owning
     * EvaluationEngine runner.
     */
    struct HGRAPH_EXPORT Graph
    {
        ~Graph();
        Graph(const Graph &) = delete;
        Graph &operator=(const Graph &) = delete;
        Graph(Graph &&other) noexcept;
        Graph &operator=(Graph &&other) noexcept;

        [[nodiscard]] const std::byte *node_layout() const noexcept
        {
            return static_cast<const std::byte *>(m_storage);
        }
        [[nodiscard]] std::span<const NodeEntry> entries() const noexcept
        {
            return {reinterpret_cast<const NodeEntry *>(m_storage), m_node_count};
        }
        [[nodiscard]] EvaluationEngineApi evaluation_engine_api() const noexcept;
        [[nodiscard]] EvaluationClock evaluation_clock() const noexcept;
        [[nodiscard]] EngineEvaluationClock engine_evaluation_clock() const noexcept;
        [[nodiscard]] Traits &traits();
        [[nodiscard]] const Traits &traits() const;
        [[nodiscard]] engine_time_t evaluation_time() const noexcept;
        [[nodiscard]] engine_time_t last_evaluation_time() const noexcept;
        [[nodiscard]] SenderReceiverState *push_message_receiver() const noexcept;
        /** Prefix length of nodes treated as push sources during evaluation. */
        [[nodiscard]] int64_t push_source_nodes_end() const noexcept;
        [[nodiscard]] engine_time_t scheduled_time(size_t index) const;
        [[nodiscard]] Node &node_at(size_t index);
        [[nodiscard]] const Node &node_at(size_t index) const;
        [[nodiscard]] PushSourceNodeRef push_source_node_at(size_t index);

        void start();
        void stop();
        void abandon() noexcept;
        void evaluate(engine_time_t when);
        void schedule_node(int64_t node_index, engine_time_t when, bool force_set = false);

      private:
        friend struct GraphBuilder;

        explicit Graph(GraphEvaluationEngine evaluation_engine) noexcept;
        void adopt_storage(void *storage,
                           size_t storage_alignment,
                           size_t node_count,
                           int64_t push_source_nodes_end) noexcept;
        void clear_storage() noexcept;
        void attach_nodes() noexcept;
        void stop_nodes(size_t nodes_end);
        [[nodiscard]] NodeEntry *entry_storage() noexcept;
        [[nodiscard]] const NodeEntry *entry_storage() const noexcept;

        size_t m_node_count{0};
        int64_t m_push_source_nodes_end{0};
        bool m_started{false};
        size_t m_storage_alignment{alignof(std::max_align_t)};
        engine_time_t m_last_evaluation_time{MIN_DT};
        GraphEvaluationEngine m_evaluation_engine;
        mutable std::unique_ptr<Traits> m_traits;
        void *m_storage{nullptr};
    };
}  // namespace hgraph::v2
