#include <hgraph/runtime/evaluation_engine.h>

namespace hgraph {
    EngineEvalautionClockDelegate::EngineEvalautionClockDelegate(
        EngineEvalautionClock *clock) : _engine_evalaution_clock{clock} {
    }

    engine_time_t EngineEvalautionClockDelegate::evaluation_time() {
        return _engine_evalaution_clock->evaluation_time();
    }

    engine_time_t EngineEvalautionClockDelegate::now() {
        return _engine_evalaution_clock->now();
    }

    engine_time_t EngineEvalautionClockDelegate::next_cycle_evaluation_time() {
        return _engine_evalaution_clock->next_cycle_evaluation_time();
    }

    engine_time_delta_t EngineEvalautionClockDelegate::cycle_time() {
        return _engine_evalaution_clock->cycle_time();
    }

    engine_time_t EngineEvalautionClockDelegate::set_evaluation_time(engine_time_t et) {
        return _engine_evalaution_clock->set_evaluation_time(et);
    }

    engine_time_t EngineEvalautionClockDelegate::next_scheduled_evaluation_time() {
        return _engine_evalaution_clock->next_scheduled_evaluation_time();
    }

    void EngineEvalautionClockDelegate::update_next_scheduled_evaluation_time(engine_time_t et) {
        _engine_evalaution_clock->update_next_scheduled_evaluation_time(et);
    }

    void EngineEvalautionClockDelegate::advance_to_next_scheduled_time() {
        _engine_evalaution_clock->advance_to_next_scheduled_time();
    }

    void EngineEvalautionClockDelegate::mark_push_node_requires_scheduling() {
        _engine_evalaution_clock->mark_push_node_requires_scheduling();
    }

    bool EngineEvalautionClockDelegate::push_node_requires_scheduling() {
        return _engine_evalaution_clock->push_node_requires_scheduling();
    }

    void EngineEvalautionClockDelegate::reset_push_node_requires_scheduling() {
        _engine_evalaution_clock->reset_push_node_requires_scheduling();
    }
}
