#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/nested_node.h>
#include <hgraph/types/graph.h>

#include <utility>

namespace hgraph {
    NestedEngineEvaluationClock::NestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock,
                                                             nested_node_ptr nested_node)
        : EngineEvaluationClockDelegate(std::move(engine_evaluation_clock)), _nested_node(nested_node) {
    }

    nested_node_ptr NestedEngineEvaluationClock::node() const { return _nested_node; }

    engine_time_t NestedEngineEvaluationClock::next_scheduled_evaluation_time() const {
        return _nested_next_scheduled_evaluation_time;
    }

    void NestedEngineEvaluationClock::reset_next_scheduled_evaluation_time() {
        _nested_next_scheduled_evaluation_time = MAX_DT;
    }

    void NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        auto let{_nested_node->last_evaluation_time()};
        auto eval_time = evaluation_time();

        // Debug output
        // std::cout << "update_next_scheduled_evaluation_time: next_time=" << next_time.time_since_epoch().count()
        //           << " let=" << let.time_since_epoch().count()
        //           << " eval_time=" << eval_time.time_since_epoch().count() << std::endl;

        //Unlike python when not set let will be MIN_DT
        // Python: if (let := self._nested_node.last_evaluation_time) and let >= next_time or self._nested_node.is_stopping:
        // In python we evaluate left to right so putting the brackets on the left side should be consistent
        if ((let != MIN_DT /* equivalent to falisy */ && let >= next_time) || _nested_node->is_stopping()) { return; }

        // Match Python: min(next_time, max(self._nested_next_scheduled_evaluation_time, (let or MIN_DT) + MIN_TD))
        // Note let or MIN_DT is equivalent to let
        // CRITICAL FIX: Also ensure we never schedule before current evaluation time
        auto min_allowed_time = std::max(eval_time, let + MIN_TD);
        auto proposed_next_time = std::min(
            next_time, std::max(_nested_next_scheduled_evaluation_time, min_allowed_time));

        if (proposed_next_time != _nested_next_scheduled_evaluation_time) {
            _nested_next_scheduled_evaluation_time = proposed_next_time;
            _nested_node->graph()->schedule_node(_nested_node->node_ndx(), proposed_next_time);
        }
    }

    NestedEvaluationEngine::NestedEvaluationEngine(EvaluationEngine::s_ptr engine,
                                                   EngineEvaluationClock::s_ptr evaluation_clock)
        : EvaluationEngineDelegate(std::move(engine)), _engine_evaluation_clock(std::move(evaluation_clock)),
          _nested_start_time(_engine_evaluation_clock->evaluation_time()) {
    }

    engine_time_t NestedEvaluationEngine::start_time() const { return _nested_start_time; }

    EvaluationClock::s_ptr NestedEvaluationEngine::evaluation_clock() { return _engine_evaluation_clock; }

    const EngineEvaluationClock::s_ptr& NestedEvaluationEngine::engine_evaluation_clock() { return _engine_evaluation_clock; }

} // namespace hgraph