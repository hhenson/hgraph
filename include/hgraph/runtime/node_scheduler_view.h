#ifndef HGRAPH_RUNTIME_NODE_SCHEDULER_VIEW_H
#define HGRAPH_RUNTIME_NODE_SCHEDULER_VIEW_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/evaluation_clock.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node_scheduler_checkpoint.h>
#include <hgraph/util/date_time.h>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <span>
#include <vector>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace hgraph
{
    /**
     * Lightweight, **stateless** one-shot scheduling injectable — the minimal
     * scheduling surface, intended mainly for ``start``. Unlike
     * :cpp:class:`NodeScheduler` it carries **no per-node state** (so a node that
     * only uses it allocates no scheduler slot and does not set ``uses_scheduler``)
     * and offers no cancellation, no tags, and no "is it scheduled?" query — it
     * merely *marks the node to evaluate* at now, a delta, or an absolute future
     * time. It never moves an existing earlier schedule later (it defers to the
     * graph's ``schedule_node`` min-semantics).
     *
     * It is a transparent injectable: it does not appear in the node's signature
     * (no input/scalar/state/kind effect) and is available to C++ nodes only — a
     * node may declare it on ``start`` without also declaring it on ``eval``.
     */
    class HGRAPH_CLASS_EXPORT SingleShotScheduler
    {
      public:
        SingleShotScheduler() noexcept = default;
        SingleShotScheduler(GraphValue *graph, std::size_t node_index, DateTime now) noexcept
            : graph_(graph), node_index_(node_index), now_(now)
        {
        }

        /** The current evaluation time (the start time when injected on ``start``). */
        [[nodiscard]] DateTime now() const noexcept { return now_; }

        /** Mark the node to evaluate in the current cycle. */
        void schedule_now() const { schedule(now_); }

        /**
         * Mark the node to evaluate at absolute time ``when``. A no-op if there is
         * no live graph; never cancels and never moves an existing earlier
         * schedule later (graph ``schedule_node`` keeps the earliest time).
         */
        void schedule(DateTime when) const
        {
            if (graph_ != nullptr) { graph_->schedule_node(node_index_, when); }
        }

        /** Mark the node to evaluate ``delta`` after the current time. */
        void schedule(TimeDelta delta) const { schedule(now_ + delta); }

      private:
        GraphValue   *graph_{nullptr};
        std::size_t   node_index_{0};
        DateTime now_{MIN_DT};
    };

    /**
     * Borrowing **view** over a node's scheduler — the injectable, built on demand
     * (state reference + node index + graph reference + current time). It mirrors
     * the 2603 ``NodeScheduler`` / Python ``SCHEDULER`` interface: schedule
     * absolute times or deltas (optionally tagged), query/replace/cancel by tag,
     * and re-arm the node. Scheduling pushes the earliest pending time onto the
     * graph; ``advance`` (run after each evaluation) consumes fired events and
     * re-arms the next.
     *
     * Wall-clock alarms (``on_wall_clock = true``) use the host clock when the
     * scheduler is injected by a real-time graph executor. Simulation and
     * manually constructed schedulers fall back to logical graph time, allowing
     * the same graph to run deterministically under either executor.
     */
    class HGRAPH_CLASS_EXPORT NodeScheduler
    {
      public:
        NodeScheduler() noexcept = default;
        NodeScheduler(NodeSchedulerState &state, GraphValue *graph, std::size_t node_index, DateTime now,
                      bool started = true, EvaluationClockView wall_clock = {},
                      bool supports_wall_clock = false) noexcept
            : state_(&state),
              graph_(graph),
              node_index_(node_index),
              now_(now),
              started_(started),
              wall_clock_(wall_clock),
              supports_wall_clock_(supports_wall_clock)
        {
        }

        /** Capture only alarms still pending after the completed cycle. */
        [[nodiscard]] NodeSchedulerCheckpoint capture_checkpoint(DateTime cut) const;

        /** Validate before the coordinator imports any node endpoints. */
        static void validate_checkpoint(const NodeSchedulerCheckpoint &image, DateTime restart);

        /** Replace start-hook alarms and re-arm through ordinary graph notification.
         * An alarm exactly at restart is due in the first resumed cycle.
         */
        void restore_checkpoint(const NodeSchedulerCheckpoint &image) const;

        /** The current evaluation time. */
        [[nodiscard]] DateTime now() const noexcept { return now_; }

        /**
         * The clock against which a scheduling request will be interpreted.
         * Wall-clock requests use host time only when the executor supports
         * them; simulation treats the request as graph-time scheduling.
         */
        [[nodiscard]] DateTime scheduling_time(bool on_wall_clock = false) const noexcept
        {
            if (!on_wall_clock || !supports_wall_clock_) { return now_; }
            return std::max(now_, wall_clock_.now());
        }

        /** Earliest pending time, or ``MIN_DT`` when none remain. During eval only future requests are pending. */
        [[nodiscard]] DateTime next_scheduled_time() const noexcept;

        /** Whether any events are pending. */
        [[nodiscard]] bool is_scheduled() const noexcept;

        /**
         * Whether the node is scheduled for the current evaluation cycle — i.e. the
         * scheduler has a delivery at this evaluation time. A simultaneous
         * input tick leaves this true, including when no later wake is pending.
         */
        [[nodiscard]] bool is_scheduled_now() const noexcept;

        /** Whether a schedule is registered under ``tag``. */
        [[nodiscard]] bool has_tag(std::string_view tag) const;

        /** Time registered under ``tag``, or ``default_time`` when absent. */
        [[nodiscard]] DateTime tag_time(std::string_view tag, DateTime default_time = MIN_DT) const;

        /** Whether ``tag``'s schedule is due in the current cycle. */
        [[nodiscard]] bool tag_is_scheduled_now(std::string_view tag) const { return has_tag(tag) && tag_time(tag) == now_; }

        /** Remove ``tag``'s event and return its time, or ``default_time`` when absent. */
        DateTime pop_tag(std::string_view tag, DateTime default_time = MIN_DT) const;

        /**
         * Schedule the node at ``when``. Once the node is started this must be
         * strictly in the future; **during ``start``** (before the node is
         * started) a node may schedule its first evaluation at the current
         * (start) time via ``schedule(now())`` — this is how a source initiates
         * itself. A non-empty ``tag`` replaces any prior event under the same tag.
         * ``on_wall_clock`` interprets ``when`` as an absolute host wall-clock
         * time in a real-time executor and as graph time in simulation. Preserves
         * the start-cycle ``schedule(now())`` source pattern before the node has
         * started.
         */
        void schedule(DateTime when, std::optional<std::string> tag = std::nullopt,
                      bool on_wall_clock = false) const;

        /** Schedule the node ``delta`` after the current evaluation time. */
        void schedule(TimeDelta delta, std::optional<std::string> tag = std::nullopt,
                      bool on_wall_clock = false) const
        {
            require_state("schedule");
            on_wall_clock = on_wall_clock && supports_wall_clock_;
            schedule(scheduling_time(on_wall_clock) + delta, std::move(tag), on_wall_clock);
        }

        /** Cancel the event registered under ``tag`` (no-op if absent). */
        void un_schedule(const std::string &tag) const;

        /** Cancel the next (earliest) pending event. */
        void un_schedule() const;

        /** Remove all pending events. */
        void reset() const;

        /**
         * Lifecycle hook run when the node fired on a scheduler event: drop the
         * fired events (``time <= now``) and re-arm the node at the next pending
         * time. This mirrors the authoritative Python ``advance`` (``while
         * events and events[0][0] <= until``); the runtime only calls it when the
         * node was ``is_scheduled_now`` for this cycle (see ``node.cpp``). Safe to
         * call when the node has no scheduler state.
         */
        void advance() const;

      private:
        void require_state(const char *what) const
        {
            if (state_ == nullptr)
            {
                throw std::logic_error(std::string{"NodeScheduler::"} + what + " requires live scheduler state");
            }
        }

        static NodeSchedulerState checkpoint_state(const NodeSchedulerCheckpoint &image, DateTime restart);
        NodeSchedulerState *state_{nullptr};
        GraphValue         *graph_{nullptr};
        std::size_t         node_index_{0};
        DateTime       now_{MIN_DT};
        bool                started_{true};
        EvaluationClockView wall_clock_{};
        bool                supports_wall_clock_{false};
    };
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NODE_SCHEDULER_VIEW_H
