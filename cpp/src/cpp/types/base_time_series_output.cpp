#include <hgraph/types/base_time_series_output.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

namespace hgraph {

    // Implement TimeSeriesType abstract interface in BaseTimeSeriesOutput
    engine_time_t BaseTimeSeriesOutput::current_engine_time() const {
        auto g = owning_graph();
        if (g != nullptr) { return g->evaluation_clock()->evaluation_time(); }
        return MIN_DT;
    }

    node_ptr BaseTimeSeriesOutput::owning_node() { return _owning_node(); }

    node_ptr BaseTimeSeriesOutput::owning_node() const { return _owning_node(); }

    graph_ptr BaseTimeSeriesOutput::owning_graph() {
        return has_owning_node() ? owning_node()->graph() : graph_ptr{};
    }

    graph_ptr BaseTimeSeriesOutput::owning_graph() const {
        return has_owning_node() ? owning_node()->graph() : graph_ptr{};
    }

    void BaseTimeSeriesOutput::re_parent(const node_ptr &parent) { _set_parent(parent); }

    void BaseTimeSeriesOutput::re_parent(const TimeSeriesType::ptr &parent) { _set_parent(parent); }

    bool BaseTimeSeriesOutput::is_reference() const { return false; }

    bool BaseTimeSeriesOutput::has_reference() const { return false; }

    void BaseTimeSeriesOutput::reset_parent_or_node() { _reset_parent_or_node(); }

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

    // --- TimeSeriesType ownership hooks (moved from TimeSeriesType) ---
    TimeSeriesType::ptr &BaseTimeSeriesOutput::_parent_time_series() {
        if (_parent_ts_or_node.has_value() && std::holds_alternative<time_series_type_ptr>(*_parent_ts_or_node)) {
            return std::get<time_series_type_ptr>(*_parent_ts_or_node);
        }
        return null_ptr;
    }

    TimeSeriesType::ptr &BaseTimeSeriesOutput::_parent_time_series() const {
        return const_cast<BaseTimeSeriesOutput *>(this)->_parent_time_series();
    }

    bool BaseTimeSeriesOutput::_has_parent_time_series() const {
        return _parent_ts_or_node.has_value() && std::holds_alternative<time_series_type_ptr>(*_parent_ts_or_node);
    }

    void BaseTimeSeriesOutput::_set_parent_time_series(TimeSeriesType *ts) { _parent_ts_or_node = time_series_type_ptr{ts}; }

    void BaseTimeSeriesOutput::_set_parent(const node_ptr &parent) { _parent_ts_or_node = parent; }

    void BaseTimeSeriesOutput::_set_parent(const TimeSeriesType::ptr &parent) { _parent_ts_or_node = parent; }

    void BaseTimeSeriesOutput::_reset_parent_or_node() { _parent_ts_or_node.reset(); }

    bool BaseTimeSeriesOutput::has_parent_or_node() const { return _parent_ts_or_node.has_value(); }

    bool BaseTimeSeriesOutput::has_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            if (std::holds_alternative<node_ptr>(*_parent_ts_or_node)) {
                return std::get<node_ptr>(*_parent_ts_or_node) != node_ptr{};
            }
            return std::get<time_series_type_ptr>(*_parent_ts_or_node)->has_owning_node();
        }
        return false;
    }

    node_ptr BaseTimeSeriesOutput::_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            return std::visit(
                []<typename T_>(T_ &&value) -> node_ptr {
                    using T = std::decay_t<T_>;
                    if constexpr (std::is_same_v<T, time_series_type_ptr>) {
                        return value->owning_node();
                    } else if constexpr (std::is_same_v<T, node_ptr>) {
                        return value;
                    } else {
                        throw std::runtime_error("Unknown type");
                    }
                },
                _parent_ts_or_node.value());
        }
        throw std::runtime_error("No node is accessible");
    }

} // namespace hgraph
