#pragma once

#include <hgraph/hgraph_base.h>

#include <stdexcept>

namespace hgraph::v2
{
    struct HGRAPH_EXPORT EvaluationClockOps
    {
        [[nodiscard]] engine_time_t (*evaluation_time)(const void *impl) noexcept;
        [[nodiscard]] engine_time_t (*now)(const void *impl);
        [[nodiscard]] engine_time_delta_t (*cycle_time)(const void *impl);
        [[nodiscard]] engine_time_t (*next_cycle_evaluation_time)(const void *impl) noexcept;
    };

    struct HGRAPH_EXPORT EngineEvaluationClockOps : EvaluationClockOps
    {
        void (*set_evaluation_time)(void *impl, engine_time_t evaluation_time);
        [[nodiscard]] engine_time_t (*next_scheduled_evaluation_time)(const void *impl) noexcept;
        void (*update_next_scheduled_evaluation_time)(void *impl, engine_time_t evaluation_time);
        void (*advance_to_next_scheduled_time)(void *impl);
        void (*mark_push_node_requires_scheduling)(void *impl);
        [[nodiscard]] bool (*push_node_requires_scheduling)(const void *impl) noexcept;
        void (*reset_push_node_requires_scheduling)(void *impl);
        [[nodiscard]] const engine_time_t *(*evaluation_time_ptr)(const void *impl) noexcept;
    };

    class HGRAPH_EXPORT EvaluationClock
    {
      public:
        EvaluationClock() = default;
        EvaluationClock(const void *impl, const EvaluationClockOps *ops) noexcept : m_impl(impl), m_ops(ops) {}

        [[nodiscard]] bool valid() const noexcept { return m_impl != nullptr && m_ops != nullptr; }
        explicit operator bool() const noexcept { return valid(); }

        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return ops().evaluation_time(m_impl); }
        [[nodiscard]] engine_time_t now() const { return ops().now(m_impl); }
        [[nodiscard]] engine_time_delta_t cycle_time() const { return ops().cycle_time(m_impl); }
        [[nodiscard]] engine_time_t next_cycle_evaluation_time() const noexcept
        {
            return ops().next_cycle_evaluation_time(m_impl);
        }

      protected:
        [[nodiscard]] const EvaluationClockOps &ops() const
        {
            if (!valid()) { throw std::logic_error("v2 EvaluationClock is not bound to runtime state"); }
            return *m_ops;
        }

        const void *m_impl{nullptr};
        const EvaluationClockOps *m_ops{nullptr};
    };

    class HGRAPH_EXPORT EngineEvaluationClock : public EvaluationClock
    {
      public:
        EngineEvaluationClock() = default;
        EngineEvaluationClock(const void *impl, const EngineEvaluationClockOps *ops) noexcept
            : EvaluationClock(impl, ops)
        {
        }

        void set_evaluation_time(engine_time_t evaluation_time) const
        {
            engine_ops().set_evaluation_time(const_cast<void *>(m_impl), evaluation_time);
        }

        [[nodiscard]] engine_time_t next_scheduled_evaluation_time() const noexcept
        {
            return engine_ops().next_scheduled_evaluation_time(m_impl);
        }

        void update_next_scheduled_evaluation_time(engine_time_t evaluation_time) const
        {
            engine_ops().update_next_scheduled_evaluation_time(const_cast<void *>(m_impl), evaluation_time);
        }

        void advance_to_next_scheduled_time() const
        {
            engine_ops().advance_to_next_scheduled_time(const_cast<void *>(m_impl));
        }

        void mark_push_node_requires_scheduling() const
        {
            engine_ops().mark_push_node_requires_scheduling(const_cast<void *>(m_impl));
        }

        [[nodiscard]] bool push_node_requires_scheduling() const noexcept
        {
            return engine_ops().push_node_requires_scheduling(m_impl);
        }

        void reset_push_node_requires_scheduling() const
        {
            engine_ops().reset_push_node_requires_scheduling(const_cast<void *>(m_impl));
        }

        [[nodiscard]] const engine_time_t *evaluation_time_ptr() const noexcept
        {
            return engine_ops().evaluation_time_ptr(m_impl);
        }

      private:
        [[nodiscard]] const EngineEvaluationClockOps &engine_ops() const
        {
            return static_cast<const EngineEvaluationClockOps &>(ops());
        }
    };
}  // namespace hgraph::v2
