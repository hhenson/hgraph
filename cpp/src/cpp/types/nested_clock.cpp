#include <hgraph/types/nested_clock.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

#include <algorithm>
#include <string_view>
#include <stdexcept>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view kNestedScheduleTag = "__nested__";

        engine_time_t evaluation_time_impl(const void *impl) noexcept
        {
            return static_cast<const NestedClockState *>(impl)->evaluation_time;
        }

        engine_time_t now_impl(const void *impl)
        {
            // Nested graphs observe the same wall-clock/simulation time as their parent.
            // The "now" for a nested graph is its evaluation_time plus any cycle offset,
            // but since nested clocks delegate cycle_time to the parent, now == evaluation_time
            // is a reasonable default for simulation mode.
            return static_cast<const NestedClockState *>(impl)->evaluation_time;
        }

        engine_time_delta_t cycle_time_impl(const void * /*impl*/)
        {
            return engine_time_delta_t{0};
        }

        engine_time_t next_cycle_evaluation_time_impl(const void *impl) noexcept
        {
            return static_cast<const NestedClockState *>(impl)->evaluation_time + MIN_TD;
        }

        void set_evaluation_time_impl(void *impl, engine_time_t et)
        {
            static_cast<NestedClockState *>(impl)->evaluation_time = et;
        }

        engine_time_t next_scheduled_evaluation_time_impl(const void *impl) noexcept
        {
            return static_cast<const NestedClockState *>(impl)->nested_next_scheduled;
        }

        /**
         * Scheduling gate for the nested clock.
         *
         * evaluation_time plays double duty here: Graph::evaluate() sets it
         * at the top of each cycle via set_evaluation_time(), so by the time
         * any child node calls schedule(), evaluation_time already reflects
         * the current tick. This lets us use it as both the "current time"
         * and the "last evaluated time" without a separate field.
         *
         * Requests at-or-before evaluation_time ask for another pass in the
         * same parent evaluation. That matches the Python nested runtime,
         * which re-enters child evaluation immediately instead of forcing the
         * work out to the next tick.
         *
         * Accepted requests are mirrored onto the owning parent node's
         * scheduler under a stable tag. That keeps Graph wake-ups and
         * Node::eval()'s scheduled-now gating in sync, including requests
         * raised during the parent's start() call.
         */
        void update_next_scheduled_evaluation_time_impl(void *impl, engine_time_t next_time)
        {
            auto *state = static_cast<NestedClockState *>(impl);

            if (state->evaluation_time != MIN_DT && next_time <= state->evaluation_time) {
                state->request_immediate_evaluation();
                return;
            }

            const engine_time_t proposed = std::min(next_time, state->nested_next_scheduled);

            if (proposed != state->nested_next_scheduled) {
                state->nested_next_scheduled = proposed;

                if (state->parent_node != nullptr) {
                    Graph *parent_graph = state->parent_node->graph();
                    if (state->parent_node->has_scheduler()) {
                        if (state->parent_node->started() && parent_graph != nullptr &&
                            proposed == parent_graph->evaluation_time()) {
                            state->parent_node->scheduler().schedule_immediate(std::string{kNestedScheduleTag});
                        } else {
                            state->parent_node->scheduler().schedule(proposed, std::string{kNestedScheduleTag});
                        }
                    } else if (state->parent_node->started() && parent_graph != nullptr) {
                        parent_graph->schedule_node(state->parent_node->node_index(), proposed);
                    }
                }
            }
        }

        void advance_to_next_scheduled_time_impl(void *impl)
        {
            auto *state = static_cast<NestedClockState *>(impl);
            state->evaluation_time = state->nested_next_scheduled;
            state->nested_next_scheduled = MAX_DT;
        }

        void mark_push_node_requires_scheduling_impl(void * /*impl*/)
        {
            throw std::logic_error("nested graphs do not support push nodes");
        }

        bool push_node_requires_scheduling_impl(const void * /*impl*/) noexcept
        {
            return false;
        }

        void reset_push_node_requires_scheduling_impl(void * /*impl*/) {}

        const engine_time_t *evaluation_time_ptr_impl(const void *impl) noexcept
        {
            return &static_cast<const NestedClockState *>(impl)->evaluation_time;
        }

        const EngineEvaluationClockOps s_nested_clock_ops{
            evaluation_time_impl,
            now_impl,
            cycle_time_impl,
            next_cycle_evaluation_time_impl,
            set_evaluation_time_impl,
            next_scheduled_evaluation_time_impl,
            update_next_scheduled_evaluation_time_impl,
            advance_to_next_scheduled_time_impl,
            mark_push_node_requires_scheduling_impl,
            push_node_requires_scheduling_impl,
            reset_push_node_requires_scheduling_impl,
            evaluation_time_ptr_impl,
        };
    }  // namespace

    const EngineEvaluationClockOps &nested_clock_ops() noexcept
    {
        return s_nested_clock_ops;
    }

}  // namespace hgraph
