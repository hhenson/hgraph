#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/v2/evaluation_clock.h>
#include <hgraph/types/value/value.h>

#include <cstdio>
#include <deque>
#include <mutex>
#include <optional>

namespace hgraph::v2
{
    /**
     * Thread-safe queue used by push source nodes to hand external messages to
     * the owning graph.
     *
     * The behavior intentionally mirrors the current Python/C++ runtime:
     * enqueueing a message marks the engine clock so the graph will wake and
     * drain pending push messages ahead of normal scheduled node evaluation.
     */
    struct HGRAPH_EXPORT SenderReceiverState
    {
        using LockType = std::recursive_mutex;
        using LockGuard = std::lock_guard<LockType>;

        /**
         * One queued push-source delivery.
         *
         * `target_node_index` identifies which push source node in the owning
         * graph should receive the message. The payload itself is an owning
         * value-layer object so queued messages stay schema-bound and
         * independent of Python object lifetimes.
         */
        struct value_type
        {
            int64_t target_node_index{-1};
            value::Value payload;
        };

        SenderReceiverState() = default;
        SenderReceiverState(const SenderReceiverState &) = delete;
        SenderReceiverState &operator=(const SenderReceiverState &) = delete;

        SenderReceiverState(SenderReceiverState &&other) noexcept
        {
            auto guard = other.guard();
            m_queue = std::move(other.m_queue);
            m_evaluation_clock = other.m_evaluation_clock;
            m_stopped = other.m_stopped;
            m_stopped_warning_emitted = other.m_stopped_warning_emitted;
            other.m_queue.clear();
            other.m_evaluation_clock = {};
            other.m_stopped = false;
            other.m_stopped_warning_emitted = false;
        }

        SenderReceiverState &operator=(SenderReceiverState &&other) noexcept
        {
            if (this == &other) { return *this; }

            auto this_guard = guard();
            auto other_guard = other.guard();
            m_queue = std::move(other.m_queue);
            m_evaluation_clock = other.m_evaluation_clock;
            m_stopped = other.m_stopped;
            m_stopped_warning_emitted = other.m_stopped_warning_emitted;
            other.m_queue.clear();
            other.m_evaluation_clock = {};
            other.m_stopped = false;
            other.m_stopped_warning_emitted = false;
            return *this;
        }

        void set_evaluation_clock(EngineEvaluationClock clock) noexcept
        {
            LockGuard guard(m_lock);
            m_evaluation_clock = clock;
        }

        void operator()(value_type value)
        {
            enqueue(std::move(value));
        }

        void enqueue(value_type value)
        {
            LockGuard guard(m_lock);
            if (m_stopped) {
                warn_ignored_enqueue_locked();
                return;
            }
            m_queue.push_back(std::move(value));
            if (m_evaluation_clock) { m_evaluation_clock.mark_push_node_requires_scheduling(); }
        }

        void enqueue_front(value_type value)
        {
            LockGuard guard(m_lock);
            if (m_stopped) {
                warn_ignored_enqueue_locked();
                return;
            }
            m_queue.push_front(std::move(value));
        }

        [[nodiscard]] std::optional<value_type> dequeue()
        {
            LockGuard guard(m_lock);
            if (m_queue.empty()) { return std::nullopt; }

            value_type value = std::move(m_queue.front());
            m_queue.pop_front();
            return value;
        }

        explicit operator bool() const
        {
            LockGuard guard(m_lock);
            return !m_queue.empty();
        }

        [[nodiscard]] bool stopped() const
        {
            LockGuard guard(m_lock);
            return m_stopped;
        }

        void mark_stopped()
        {
            LockGuard guard(m_lock);
            m_stopped = true;
        }

        [[nodiscard]] auto guard() const -> LockGuard
        {
            return LockGuard(m_lock);
        }

      private:
        void warn_ignored_enqueue_locked()
        {
            if (m_stopped_warning_emitted) { return; }

            std::fprintf(stderr, "Warning: ignoring enqueue into a stopped receiver\n");
            m_stopped_warning_emitted = true;
        }

        mutable LockType m_lock;
        std::deque<value_type> m_queue;
        EngineEvaluationClock m_evaluation_clock;
        bool m_stopped{false};
        bool m_stopped_warning_emitted{false};
    };
}  // namespace hgraph::v2
