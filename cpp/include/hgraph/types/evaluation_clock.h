#pragma once

#include <hgraph/hgraph_base.h>

#include <stdexcept>

namespace hgraph
{
    /** Type-erased read-only clock surface injected into nodes. */
    struct HGRAPH_EXPORT EvaluationClockOps
    {
        [[nodiscard]] engine_time_t (*evaluation_time)(const void *impl) noexcept;
        [[nodiscard]] engine_time_t (*now)(const void *impl);
        [[nodiscard]] engine_time_delta_t (*cycle_time)(const void *impl);
        [[nodiscard]] engine_time_t (*next_cycle_evaluation_time)(const void *impl) noexcept;
    };

    /** Internal mutable clock surface used by Graph and EvaluationEngine. */
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

    /**
     * View on time for the currently evaluating graph.
     *
     * The clock abstracts over the execution mode so node logic can reason
     * about time without caring whether the engine is simulating historical
     * data or running against wall-clock time. In simulation mode the clock is
     * advanced by the engine. In real-time mode it reflects the system clock in
     * UTC.
     *
     * Typical usage inside a static node implementation:
     *
     * @code
     * struct EvaluationTimeNode
     * {
     *     static void eval(In<"ts", SIGNAL> ts, EvaluationClock clock, Out<TS<engine_time_t>> out)
     *     {
     *         static_cast<void>(ts);
     *         out.set(clock.evaluation_time());
     *     }
     * };
     * @endcode
     */
    class HGRAPH_EXPORT EvaluationClock
    {
      public:
        EvaluationClock() = default;
        EvaluationClock(const void *impl, const EvaluationClockOps *ops) noexcept : m_impl(impl), m_ops(ops) {}

        /** True when the facade is bound to concrete runtime clock state. */
        [[nodiscard]] bool valid() const noexcept { return m_impl != nullptr && m_ops != nullptr; }
        /** Convenience validity check for `if (clock) { ... }` style usage. */
        explicit operator bool() const noexcept { return valid(); }

        /**
         * Time of the source event initiating the current evaluation cycle.
         *
         * This value stays constant for every node processed in the same graph
         * evaluation step.
         *
         * Example:
         *
         * @code
         * if (clock.evaluation_time() == some_expected_time) { ... }
         * @endcode
         */
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return ops().evaluation_time(m_impl); }

        /**
         * Current time as observed by the engine.
         *
         * In real-time mode this tracks the system clock. In simulation mode it
         * is typically `evaluation_time() + cycle_time()`.
         *
         * Example:
         *
         * @code
         * const bool lagged = clock.now() > clock.evaluation_time();
         * @endcode
         */
        [[nodiscard]] engine_time_t now() const { return ops().now(m_impl); }

        /**
         * Elapsed computation time since the start of the current evaluation
         * cycle.
         *
         * Example:
         *
         * @code
         * if (clock.cycle_time() > std::chrono::milliseconds{10}) { ... }
         * @endcode
         */
        [[nodiscard]] engine_time_delta_t cycle_time() const { return ops().cycle_time(m_impl); }

        /**
         * Smallest evaluation time that can be scheduled for the next cycle.
         *
         * This is a convenience for `evaluation_time() + MIN_TD`.
         *
         * Example:
         *
         * @code
         * const engine_time_t next_tick = clock.next_cycle_evaluation_time();
         * @endcode
         */
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

    /**
     * Mutable engine-facing extension of EvaluationClock.
     *
     * This interface is for the graph engine and nested engines, not for
     * ordinary node logic. It adds the scheduling and advancement operations
     * needed to drive the evaluation loop.
     *
     * Example engine-side usage:
     *
     * @code
     * EngineEvaluationClock clock = graph.engine_evaluation_clock();
     * clock.set_evaluation_time(start_time);
     * graph.evaluate(clock.evaluation_time());
     * clock.advance_to_next_scheduled_time();
     * @endcode
     */
    class HGRAPH_EXPORT EngineEvaluationClock : public EvaluationClock
    {
      public:
        EngineEvaluationClock() = default;
        EngineEvaluationClock(const void *impl, const EngineEvaluationClockOps *ops) noexcept
            : EvaluationClock(impl, ops)
        {
        }

        /**
         * Set the evaluation time for the current cycle.
         *
         * This should only be called by engine code coordinating graph
         * execution.
         */
        void set_evaluation_time(engine_time_t evaluation_time) const
        {
            engine_ops().set_evaluation_time(const_cast<void *>(m_impl), evaluation_time);
        }

        /**
         * Earliest time for which a node is currently scheduled to run.
         *
         * Example:
         *
         * @code
         * if (clock.next_scheduled_evaluation_time() < end_time) { ... }
         * @endcode
         */
        [[nodiscard]] engine_time_t next_scheduled_evaluation_time() const noexcept
        {
            return engine_ops().next_scheduled_evaluation_time(m_impl);
        }

        /**
         * Fold a newly scheduled time into the engine's next scheduled
         * evaluation time.
         *
         * Implementations keep the earliest valid future time.
         */
        void update_next_scheduled_evaluation_time(engine_time_t evaluation_time) const
        {
            engine_ops().update_next_scheduled_evaluation_time(const_cast<void *>(m_impl), evaluation_time);
        }

        /**
         * Advance the engine to the next scheduled evaluation time.
         *
         * In simulation mode this normally just moves evaluation_time to the
         * next scheduled time. In real-time mode this may block until the next
         * scheduled time or until push-node activity forces the engine forward.
         */
        void advance_to_next_scheduled_time() const
        {
            engine_ops().advance_to_next_scheduled_time(const_cast<void *>(m_impl));
        }

        /**
         * Mark that one or more push nodes require scheduling.
         *
         * Push nodes are only meaningful in real-time execution. Simulation
         * engines are expected to reject this operation.
         */
        void mark_push_node_requires_scheduling() const
        {
            engine_ops().mark_push_node_requires_scheduling(const_cast<void *>(m_impl));
        }

        /**
         * True when push nodes have requested scheduling.
         *
         * Example:
         *
         * @code
         * if (clock.push_node_requires_scheduling()) { ... }
         * @endcode
         */
        [[nodiscard]] bool push_node_requires_scheduling() const noexcept
        {
            return engine_ops().push_node_requires_scheduling(m_impl);
        }

        /** Clear the pending push-node scheduling flag. */
        void reset_push_node_requires_scheduling() const
        {
            engine_ops().reset_push_node_requires_scheduling(const_cast<void *>(m_impl));
        }

        /**
         * Direct pointer to the underlying evaluation time.
         *
         * This exists as a performance hook for code that caches the current
         * evaluation time repeatedly and wants to avoid repeated virtual /
         * indirect dispatch.
         */
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
}  // namespace hgraph
