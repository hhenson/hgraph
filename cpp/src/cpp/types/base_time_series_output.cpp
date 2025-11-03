#include <hgraph/types/base_time_series_output.h>
#include <hgraph/types/graph.h>

namespace hgraph {

    void BaseTimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<BaseTimeSeriesOutput, TimeSeriesOutput>(m, "BaseTimeSeriesOutput");
    }

    bool BaseTimeSeriesOutput::modified() const {
        auto g = owning_graph();
        if (!g) { return false; }
        return g->evaluation_clock()->evaluation_time() == _last_modified_time;
    }

    engine_time_t BaseTimeSeriesOutput::last_modified_time() const { return _last_modified_time; }

    void BaseTimeSeriesOutput::mark_invalid() {
        if (_last_modified_time > MIN_DT) {
            _last_modified_time = MIN_DT;
            auto g = owning_graph();
            if (g) {
                _notify(g->evaluation_clock()->evaluation_time());
            } else {
                // Owning graph not yet attached; skip notify to avoid dereferencing null during start/recover
            }
        }
    }

    void BaseTimeSeriesOutput::mark_modified() {
        if (has_parent_or_node()) {
            auto g = owning_graph();
            if (g != nullptr) {
                mark_modified(g->evaluation_clock()->evaluation_time());
            } else {
                // Graph not yet attached; mark with a maximal time to preserve monotonicity without dereferencing
                mark_modified(MAX_ET);
            }
        } else {
            mark_modified(MAX_ET);
        }
    }

    void BaseTimeSeriesOutput::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        mark_modified(modified_time);
    }

    bool BaseTimeSeriesOutput::valid() const { return _last_modified_time > MIN_DT; }

    bool BaseTimeSeriesOutput::all_valid() const { return valid(); }

    TimeSeriesOutput::ptr BaseTimeSeriesOutput::parent_output() const {
        return static_cast<TimeSeriesOutput *>(_parent_time_series().get());
    }

    TimeSeriesOutput::ptr BaseTimeSeriesOutput::parent_output() {
        return static_cast<TimeSeriesOutput *>(_parent_time_series().get());
    }

    bool BaseTimeSeriesOutput::has_parent_output() const { return _has_parent_time_series(); }

    void BaseTimeSeriesOutput::subscribe(Notifiable *notifiable) { _subscribers.insert(notifiable); }

    void BaseTimeSeriesOutput::un_subscribe(Notifiable *notifiable) { _subscribers.erase(notifiable); }

    void BaseTimeSeriesOutput::builder_release_cleanup() {
        // Clear subscribers safely without notifications
        _subscribers.clear();
        // Reset modification state to a neutral value without touching evaluation_clock
        _reset_last_modified_time();
    }

    bool BaseTimeSeriesOutput::can_apply_result(nb::object) { return !modified(); }

    void BaseTimeSeriesOutput::clear() {}

    void BaseTimeSeriesOutput::invalidate() { mark_invalid(); }

    void BaseTimeSeriesOutput::mark_modified(engine_time_t modified_time) {
        if (_last_modified_time < modified_time) {
            _last_modified_time = modified_time;
            if (has_parent_output()) { parent_output()->mark_child_modified(*this, modified_time); }
            _notify(modified_time);
        }
    }

    void BaseTimeSeriesOutput::notify(engine_time_t et) { mark_modified(et); }

    void BaseTimeSeriesOutput::_notify(engine_time_t modified_time) {
        for (auto *subscriber: _subscribers) { subscriber->notify(modified_time); }
    }

    void BaseTimeSeriesOutput::_reset_last_modified_time() { _last_modified_time = MIN_DT; }

} // namespace hgraph
