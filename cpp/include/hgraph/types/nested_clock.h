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
     * `evaluation_time` is the current nested cycle time. Requests at or before
     * the current cycle are ignored. Requests raised while the owning nested
     * graph is stopping are also ignored. Accepted future requests are mirrored
     * onto the owning parent node's scheduler. Nested graphs therefore remain
     * single-pass per parent evaluation; they never request a private same-tick
     * replay inside `ChildGraphInstance::evaluate()`.
     */
    struct HGRAPH_EXPORT NestedClockState
    {
        Node *parent_node{nullptr};                 ///< Owning nested-operator node in the parent graph.
        engine_time_t nested_next_scheduled{MAX_DT};  ///< Earliest requested child evaluation time.
        engine_time_t evaluation_time{MIN_DT};      ///< Current nested cycle time.
        bool is_stopping{false};                    ///< Mirrors Python nested clock suppression during stop().

        void reset_next_scheduled() noexcept { nested_next_scheduled = MAX_DT; }
    };

    /**
     * Returns the EngineEvaluationClockOps vtable for NestedClockState.
     *
     * Use with EngineEvaluationClock{&state, &nested_clock_ops()} to create
     * a clock facade backed by nested clock state.
     */
    [[nodiscard]] HGRAPH_EXPORT const EngineEvaluationClockOps &nested_clock_ops() noexcept;

}  // namespace hgraph
