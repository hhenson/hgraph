#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/v2/evaluation_engine.h>
#include <hgraph/types/v2/node.h>

#include <cstddef>
#include <span>

namespace hgraph::v2
{
    /** Schedule entry pairing the next requested evaluation time with a node. */
    struct HGRAPH_EXPORT NodeEntry
    {
        engine_time_t scheduled{MIN_DT};
        Node *node{nullptr};
    };

    /**
     * Runtime graph owning the v2 node slab.
     *
     * Graph owns one contiguous storage block containing NodeEntry[N] followed
     * by the variable-sized node chunks created by GraphBuilder. Evaluation is
     * driven through an attached EvaluationEngine supplied by the owning runner.
     */
    struct HGRAPH_EXPORT Graph
    {
        Graph() = default;
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
        [[nodiscard]] engine_time_t evaluation_time() const noexcept;
        [[nodiscard]] engine_time_t scheduled_time(size_t index) const;
        [[nodiscard]] Node &node_at(size_t index);
        [[nodiscard]] const Node &node_at(size_t index) const;

        void start();
        void stop();
        void evaluate(engine_time_t when);
        void schedule_node(int64_t node_index, engine_time_t when);

      private:
        friend struct GraphBuilder;
        friend class EvaluationEngineBuilder;

        void set_evaluation_engine(void *evaluation_engine_impl, const EvaluationEngineOps *evaluation_engine_ops);
        void adopt_storage(void *storage,
                           size_t storage_alignment,
                           size_t node_count) noexcept;
        void clear_storage() noexcept;
        void attach_nodes() noexcept;
        [[nodiscard]] NodeEntry *entry_storage() noexcept;
        [[nodiscard]] const NodeEntry *entry_storage() const noexcept;

        size_t m_node_count{0};
        bool m_started{false};
        size_t m_storage_alignment{alignof(std::max_align_t)};
        void *m_evaluation_engine_impl{nullptr};
        const EvaluationEngineOps *m_evaluation_engine_ops{nullptr};
        void *m_storage{nullptr};
    };
}  // namespace hgraph::v2
