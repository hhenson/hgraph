#include <hgraph/types/graph.h>
#include <hgraph/types/nested_clock.h>
#include <hgraph/types/node.h>

#include <algorithm>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view kNestedScheduleTag = "__nested__";

        [[nodiscard]] EngineEvaluationClock parent_engine_clock(const NestedClockState *state) noexcept {
            if (state == nullptr || state->parent_node == nullptr || state->parent_node->graph() == nullptr) {
                return {};
            }
            return state->parent_node->graph()->engine_evaluation_clock();
        }

        engine_time_t evaluation_time_impl(const void *impl) noexcept {
            auto *state = static_cast<const NestedClockState *>(impl);
            auto  clock = parent_engine_clock(state);
            return clock ? clock.evaluation_time() : state->evaluation_time;
        }

        engine_time_t now_impl(const void *impl) {
            auto *state = static_cast<const NestedClockState *>(impl);
            auto  clock = parent_engine_clock(state);
            return clock ? clock.now() : state->evaluation_time;
        }

        engine_time_delta_t cycle_time_impl(const void *impl) {
            auto *state = static_cast<const NestedClockState *>(impl);
            auto  clock = parent_engine_clock(state);
            return clock ? clock.cycle_time() : engine_time_delta_t{0};
        }

        engine_time_t next_cycle_evaluation_time_impl(const void *impl) noexcept {
            auto *state = static_cast<const NestedClockState *>(impl);
            auto  clock = parent_engine_clock(state);
            return clock ? clock.next_cycle_evaluation_time() : state->evaluation_time + MIN_TD;
        }

        void set_evaluation_time_impl(void *impl, engine_time_t et) { static_cast<NestedClockState *>(impl)->evaluation_time = et; }

        engine_time_t next_scheduled_evaluation_time_impl(const void *impl) noexcept {
            return static_cast<const NestedClockState *>(impl)->nested_next_scheduled;
        }

        void update_next_scheduled_evaluation_time_impl(void *impl, engine_time_t next_time) {
            auto *state = static_cast<NestedClockState *>(impl);

            if (state->is_stopping) { return; }

            // Match Python NestedEngineEvaluationClock: stale requests are
            // compared against the parent nested node's last evaluation, not a
            // child-local clock value that may be stale between parent ticks.
            const engine_time_t last_eval = state->last_evaluation_time;
            if (last_eval != MIN_DT && last_eval >= next_time) { return; }

            const engine_time_t floor    = (last_eval != MIN_DT ? last_eval : MIN_DT) + MIN_TD;
            const engine_time_t proposed = std::min(next_time, std::max(state->nested_next_scheduled, floor));

            if (proposed != state->nested_next_scheduled) {
                state->nested_next_scheduled = proposed;

                if (state->parent_node != nullptr) {
                    Graph *parent_graph = state->parent_node->graph();
                    if (state->parent_node->has_scheduler()) {
                        const engine_time_t parent_time = parent_graph != nullptr ? parent_graph->evaluation_time() : MIN_DT;
                        if (parent_time != MIN_DT && proposed == parent_time && state->parent_node->started()) {
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

        void advance_to_next_scheduled_time_impl(void *impl) {
            auto *state                  = static_cast<NestedClockState *>(impl);
            state->evaluation_time       = state->nested_next_scheduled;
            state->nested_next_scheduled = MAX_DT;
        }

        void mark_push_node_requires_scheduling_impl(void * /*impl*/) {
            throw std::logic_error("nested graphs do not support push nodes");
        }

        bool push_node_requires_scheduling_impl(const void * /*impl*/) noexcept { return false; }

        void reset_push_node_requires_scheduling_impl(void * /*impl*/) {}

        const engine_time_t *evaluation_time_ptr_impl(const void *impl) noexcept {
            return &static_cast<const NestedClockState *>(impl)->evaluation_time;
        }

        bool set_alarm_impl(void *, engine_time_t, std::string, std::function<void(engine_time_t)>) { return false; }

        void cancel_alarm_impl(void *, std::string_view) {}

        const EngineEvaluationClockOps s_nested_clock_ops{
            {
                evaluation_time_impl,
                now_impl,
                cycle_time_impl,
                next_cycle_evaluation_time_impl,
            },
            set_evaluation_time_impl,
            next_scheduled_evaluation_time_impl,
            update_next_scheduled_evaluation_time_impl,
            advance_to_next_scheduled_time_impl,
            mark_push_node_requires_scheduling_impl,
            push_node_requires_scheduling_impl,
            reset_push_node_requires_scheduling_impl,
            evaluation_time_ptr_impl,
            set_alarm_impl,
            cancel_alarm_impl,
        };
    }  // namespace

    const EngineEvaluationClockOps &nested_clock_ops() noexcept { return s_nested_clock_ops; }

}  // namespace hgraph
