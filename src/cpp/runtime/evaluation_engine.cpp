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

    EvaluationEngineDelegate::EvaluationEngineDelegate(EvaluationEngine *api) : _evaluation_engine{api} {
    }

    EvaluationMode EvaluationEngineDelegate::evaluation_mode() const {
        return _evaluation_engine->evaluation_mode();
    }

    engine_time_t EvaluationEngineDelegate::start_time() const {
        return _evaluation_engine->start_time();
    }

    engine_time_t EvaluationEngineDelegate::end_time() const {
        return _evaluation_engine->end_time();
    }

    EvaluationClock &EvaluationEngineDelegate::evaluation_clock() {
        return _evaluation_engine->evaluation_clock();
    }

    EngineEvalautionClock &EvaluationEngineDelegate::engine_evaluation_clock() {
        return _evaluation_engine->engine_evaluation_clock();
    }

    void EvaluationEngineDelegate::request_engine_stop() {
        _evaluation_engine->request_engine_stop();
    }

    bool EvaluationEngineDelegate::is_stop_requested() {
        return _evaluation_engine->is_stop_requested();
    }

    void EvaluationEngineDelegate::add_before_evalaution_notification(std::function<void()> &&fn) {
        _evaluation_engine->add_before_evalaution_notification(std::forward<std::function<void()> >(fn));
    }

    void EvaluationEngineDelegate::add_after_evalaution_notification(std::function<void()> &&fn) {
        _evaluation_engine->add_after_evalaution_notification(std::forward<std::function<void()> >(fn));
    }

    void EvaluationEngineDelegate::add_life_cycle_observer(EvaluationLifeCycleObserver::s_ptr observer) {
        _evaluation_engine->add_life_cycle_observer(std::move(observer));
    }

    void EvaluationEngineDelegate::remove_life_cycle_observer(EvaluationLifeCycleObserver::s_ptr observer) {
        _evaluation_engine->remove_life_cycle_observer(std::move(observer));
    }

    void EvaluationEngineDelegate::advance_engine_time() {
        _evaluation_engine->advance_engine_time();
    }

    void EvaluationEngineDelegate::notify_before_evaluation() {
        _evaluation_engine->notify_before_evaluation();
    }

    void EvaluationEngineDelegate::notify_after_evaluation() {
        _evaluation_engine->notify_after_evaluation();
    }

    void EvaluationEngineDelegate::notify_before_start_graph(Graph &graph) {
        _evaluation_engine->notify_before_start_graph(graph);
    }

    void EvaluationEngineDelegate::notify_after_start_graph(Graph &graph) {
        _evaluation_engine->notify_after_start_graph(graph);
    }

    void EvaluationEngineDelegate::notify_before_start_node(Node &node) {
        _evaluation_engine->notify_before_start_node(node);
    }

    void EvaluationEngineDelegate::notify_after_start_node(Node &node) {
        _evaluation_engine->notify_after_start_node(node);
    }

    void EvaluationEngineDelegate::notify_before_graph_evaluation(Graph &graph) {
        _evaluation_engine->notify_before_graph_evaluation(graph);
    }

    void EvaluationEngineDelegate::notify_after_graph_evaluation(Graph &graph) {
        _evaluation_engine->notify_after_graph_evaluation(graph);
    }

    void EvaluationEngineDelegate::notify_before_node_evaluation(Node &node) {
        _evaluation_engine->notify_before_node_evaluation(node);
    }

    void EvaluationEngineDelegate::notify_after_node_evaluation(Node &node) {
        _evaluation_engine->notify_after_node_evaluation(node);
    }

    void EvaluationEngineDelegate::notify_before_stop_node(Node &node) {
        _evaluation_engine->notify_before_stop_node(node);
    }

    void EvaluationEngineDelegate::notify_after_stop_node(Node &node) {
        _evaluation_engine->notify_after_stop_node(node);
    }

    void EvaluationEngineDelegate::notify_before_stop_graph(Graph &graph) {
        _evaluation_engine->notify_before_stop_graph(graph);
    }

    void EvaluationEngineDelegate::notify_after_stop_graph(Graph &graph) {
        _evaluation_engine->notify_after_stop_graph(graph);
    }

    void EvaluationEngineDelegate::initialise() {
        _evaluation_engine->initialise();
    }

    void EvaluationEngineDelegate::start() {
        _evaluation_engine->start();
    }

    void EvaluationEngineDelegate::stop() {
        _evaluation_engine->stop();
    }

    void EvaluationEngineDelegate::dispose() {
        _evaluation_engine->dispose();
    }
}
