#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/evaluation_clock.h>

namespace hgraph
{
    struct Graph;
    struct Node;

    /**
     * Concrete clock state for a nested child graph.
     *
     * The scheduling guard uses evaluation_time to serve double duty:
     *
     * 1. It is the current cycle time — Graph::evaluate() sets it via
     *    set_evaluation_time() before any child nodes run.
     * 2. It is the scheduling floor — update_next_scheduled rejects
     *    requests at-or-before evaluation_time and floors the proposed
     *    time to evaluation_time + MIN_TD.
     *
     * This works because Graph::evaluate() sets evaluation_time at the
     * top of the cycle, so by the time any child node calls schedule(),
     * evaluation_time already reflects the current tick. After evaluate()
     * returns, evaluation_time remains at that tick, preventing any
     * stale re-scheduling at the same time.
     *
     * When the nested clock accepts a scheduling request it delegates to
     * the owning parent node's scheduler so that the parent graph wake-up
     * and the node's own scheduled-now gating stay consistent.
     */
    struct HGRAPH_EXPORT NestedClockState
    {
        Node *parent_node{nullptr};             ///< Owning nested-operator node in the parent graph.
        engine_time_t nested_next_scheduled{MAX_DT};  ///< Earliest requested child evaluation time.
        engine_time_t evaluation_time{MIN_DT};  ///< Current cycle time AND scheduling floor.
        bool immediate_evaluation_requested{false};

        void reset_next_scheduled() noexcept {
            nested_next_scheduled = MAX_DT;
            immediate_evaluation_requested = false;
        }
        void request_immediate_evaluation() noexcept { immediate_evaluation_requested = true; }
        [[nodiscard]] bool consume_immediate_evaluation_request() noexcept {
            const bool requested = immediate_evaluation_requested;
            immediate_evaluation_requested = false;
            return requested;
        }
    };

    /**
     * Returns the EngineEvaluationClockOps vtable for NestedClockState.
     *
     * Use with EngineEvaluationClock{&state, &nested_clock_ops()} to create
     * a clock facade backed by nested clock state.
     */
    [[nodiscard]] HGRAPH_EXPORT const EngineEvaluationClockOps &nested_clock_ops() noexcept;

}  // namespace hgraph
