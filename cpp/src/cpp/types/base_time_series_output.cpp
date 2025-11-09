#include <hgraph/types/base_time_series_output.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

namespace hgraph {

    node_ptr BaseTimeSeriesOutput::owning_node() { return _owning_node(); }

    node_ptr BaseTimeSeriesOutput::owning_node() const { return _owning_node(); }

    graph_ptr BaseTimeSeriesOutput::owning_graph() {
        return has_owning_node() ? owning_node()->graph() : graph_ptr{};
    }

    graph_ptr BaseTimeSeriesOutput::owning_graph() const {
        return has_owning_node() ? owning_node()->graph() : graph_ptr{};
    }

    void BaseTimeSeriesOutput::re_parent(const node_ptr &parent) { _parent_ts_or_node = parent; }

    void BaseTimeSeriesOutput::re_parent(const TimeSeriesType::ptr &parent) { _parent_ts_or_node = parent; }

    bool BaseTimeSeriesOutput::is_reference() const { return false; }

    bool BaseTimeSeriesOutput::has_reference() const { return false; }

    void BaseTimeSeriesOutput::reset_parent_or_node() { _parent_ts_or_node.reset(); }

    bool BaseTimeSeriesOutput::has_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            if (std::holds_alternative<node_ptr>(*_parent_ts_or_node)) {
                return std::get<node_ptr>(*_parent_ts_or_node) != node_ptr{};
            }
            return std::get<TimeSeriesType::ptr>(*_parent_ts_or_node)->has_owning_node();
        } else {
            return false;
        }
    }

    void BaseTimeSeriesOutput::notify(engine_time_t et) { mark_modified(et); }

    TimeSeriesOutput::ptr BaseTimeSeriesOutput::parent_output() const {
        return static_cast<TimeSeriesOutput *>(_parent_time_series().get()); // NOLINT(*-pro-type-static-cast-downcast)
    }

    TimeSeriesOutput::ptr BaseTimeSeriesOutput::parent_output() {
        return static_cast<TimeSeriesOutput *>(_parent_time_series().get()); // NOLINT(*-pro-type-static-cast-downcast)
    }

    bool BaseTimeSeriesOutput::has_parent_output() const { return _has_parent_time_series(); }

    bool BaseTimeSeriesOutput::can_apply_result(nb::object value) {
        return not modified();
    }

    // Minimal-teardown helper: avoid consulting owning_node/graph
    void BaseTimeSeriesOutput::builder_release_cleanup() {
        // Clear subscribers safely without notifications
        _subscribers.clear();
        // Reset modification state to a neutral value without touching evaluation_clock
        _reset_last_modified_time();
    }

    bool BaseTimeSeriesOutput::modified() const {
        auto g = owning_graph();
        if (!g) { return false; }
        return g->evaluation_clock()->evaluation_time() == _last_modified_time;
    }

    bool BaseTimeSeriesOutput::valid() const { return _last_modified_time > MIN_DT; }

    bool BaseTimeSeriesOutput::all_valid() const {
        return valid(); // By default, all valid is the same as valid
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
                // This is a bad situation, I would probably prefer to find out why,
                // TODO: find the root cause of why this could be called without a bound graph.
            }
        } else {
            mark_modified(MAX_ET);
        }
    }

    void BaseTimeSeriesOutput::mark_modified(engine_time_t modified_time) { // NOLINT(*-no-recursion)
        if (_last_modified_time < modified_time) {
            _last_modified_time = modified_time;
            if (has_parent_output()) { parent_output()->mark_child_modified(*this, modified_time); }
            _notify(modified_time);
        }
    }

    void BaseTimeSeriesOutput::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        mark_modified(modified_time);
    } // NOLINT(*-no-recursion)

    void BaseTimeSeriesOutput::subscribe(Notifiable *notifiable) { _subscribers.insert(notifiable); }

    void BaseTimeSeriesOutput::un_subscribe(Notifiable *notifiable) { _subscribers.erase(notifiable); }

    void BaseTimeSeriesOutput::_notify(engine_time_t modified_time) {
        for (auto *subscriber: _subscribers) { subscriber->notify(modified_time); }
    }

    void BaseTimeSeriesOutput::_reset_last_modified_time() { _last_modified_time = MIN_DT; }

    void BaseTimeSeriesOutput::clear() {
    }

    void BaseTimeSeriesOutput::invalidate() { mark_invalid(); }

    void BaseTimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<BaseTimeSeriesOutput, TimeSeriesOutput>(m, "BaseTimeSeriesOutput");
    }

} // namespace hgraph
