#include <hgraph/types/base_time_series.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <iostream>
#include <execinfo.h>

namespace hgraph {

    // ============================================================================
    // BaseTimeSeriesOutput Implementation
    // ============================================================================

    // Implement TimeSeriesType pure virtuals for Output
    Node* BaseTimeSeriesOutput::owning_node() { return _owning_node(); }
    Node* BaseTimeSeriesOutput::owning_node() const { return _owning_node(); }

    Graph* BaseTimeSeriesOutput::owning_graph() {
        return has_owning_node() ? owning_node()->graph() : nullptr;
    }

    Graph* BaseTimeSeriesOutput::owning_graph() const {
        return has_owning_node() ? owning_node()->graph() : nullptr;
    }

    bool BaseTimeSeriesOutput::is_reference() const { return false; }
    bool BaseTimeSeriesOutput::has_reference() const { return false; }

    void BaseTimeSeriesOutput::reset_parent_or_node() { _parent_ts_or_node.reset(); }
    
    // Implement re_parent methods
    void BaseTimeSeriesOutput::re_parent(node_ptr parent) {
        _parent_ts_or_node = parent;
    }
    void BaseTimeSeriesOutput::re_parent(const time_series_type_ptr parent) {
        _parent_ts_or_node = static_cast<time_series_output_ptr>(parent);
    }

    // TimeSeriesType helper access methods
    time_series_output_ptr BaseTimeSeriesOutput::_parent_output() const {
        return const_cast<BaseTimeSeriesOutput *>(this)->_parent_output();
    }

    time_series_output_ptr BaseTimeSeriesOutput::_parent_output() {
        if (_parent_ts_or_node.has_value() && std::holds_alternative<time_series_output_ptr>(*_parent_ts_or_node)) {
            return std::get<time_series_output_ptr>(*_parent_ts_or_node);
        } else {
            return nullptr;
        }
    }

    bool BaseTimeSeriesOutput::_has_parent_output() const {
        return _parent_ts_or_node.has_value() && std::holds_alternative<time_series_output_ptr>(*_parent_ts_or_node);
    }

    void BaseTimeSeriesOutput::_set_parent_output(time_series_output_ptr ts) {
        if (_parent_ts_or_node.has_value() && std::holds_alternative<time_series_output_ptr>(*_parent_ts_or_node)) {
            std::get<time_series_output_ptr>(*_parent_ts_or_node) = ts;
        } else {
            _parent_ts_or_node = ts;
        }
    }

    bool BaseTimeSeriesOutput::has_parent_or_node() const { return _parent_ts_or_node.has_value(); }

    bool BaseTimeSeriesOutput::has_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            if (std::holds_alternative<node_ptr>(*_parent_ts_or_node)) {
                return std::get<node_ptr>(*_parent_ts_or_node) != nullptr;
            }
            return std::get<time_series_output_ptr>(*_parent_ts_or_node)->has_owning_node();
        } else {
            return false;
        }
    }

    Node* BaseTimeSeriesOutput::_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            return std::visit(
                []<typename T_>(T_ &&value) -> Node* {
                    using T = std::decay_t<T_>;
                    if constexpr (std::is_same_v<T, time_series_output_ptr>) {
                        return value->owning_node();
                    } else if constexpr (std::is_same_v<T, node_ptr>) {
                        return value;
                    } else {
                        throw std::runtime_error("Unknown type");
                    }
                },
                _parent_ts_or_node.value());
        } else {
            throw std::runtime_error("No node is accessible");
        }
    }

    void BaseTimeSeriesOutput::clear() {
    }

    void BaseTimeSeriesOutput::invalidate() { mark_invalid(); }

    TimeSeriesOutput::s_ptr BaseTimeSeriesOutput::parent_output() const {
        if (_has_parent_output()) {
            auto p = std::get<time_series_output_ptr>(*_parent_ts_or_node);
            return p ? p->shared_from_this() : time_series_output_s_ptr{};
        }
        return {};
    }

    TimeSeriesOutput::s_ptr BaseTimeSeriesOutput::parent_output() {
        return const_cast<const BaseTimeSeriesOutput *>(this)->parent_output();
    }

    bool BaseTimeSeriesOutput::has_parent_output() const { return _has_parent_output(); }

    bool BaseTimeSeriesOutput::can_apply_result(const nb::object& value) {
        return !modified();
    }

    void BaseTimeSeriesOutput::builder_release_cleanup() {
        // Clear subscribers safely without notifications
        _subscribers.clear();
        // Reset modification state to a neutral value without touching evaluation_clock
        _reset_last_modified_time();
    }

    bool BaseTimeSeriesOutput::modified() const {
        auto n = owning_node();
        if (n == nullptr) { return false; }
        // Use cached evaluation time pointer from node for performance
        return *n->cached_evaluation_time_ptr() == _last_modified_time;
    }

    bool BaseTimeSeriesOutput::valid() const { return _last_modified_time > MIN_DT; }

    bool BaseTimeSeriesOutput::all_valid() const {
        return valid(); // By default, all valid is the same as valid
    }

    engine_time_t BaseTimeSeriesOutput::last_modified_time() const { return _last_modified_time; }

    void BaseTimeSeriesOutput::mark_invalid() {
        if (_last_modified_time > MIN_DT) {
            _last_modified_time = MIN_DT;
            auto n = owning_node();
            if (n != nullptr) {
                // Use cached evaluation time pointer from node for performance
                _notify(*n->cached_evaluation_time_ptr());
            } else {
                // Owning node not yet attached; skip notify to avoid dereferencing null during start/recover
            }
        }
    }

    void BaseTimeSeriesOutput::mark_modified() {
        if (has_parent_or_node()) {
            auto n = owning_node();
            if (n != nullptr) {
                // Use cached evaluation time pointer from node for performance
                mark_modified(*n->cached_evaluation_time_ptr());
            } else {
                // Owning node not yet attached; mark with a maximal time to preserve monotonicity without dereferencing
                // This is a bad situation, I would probably prefer to find out why,
                // TODO: find the root cause of why this could be called without a bound node.
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

    void BaseTimeSeriesOutput::un_subscribe(Notifiable *notifiable) {
        _subscribers.erase(notifiable);
    }

    void BaseTimeSeriesOutput::_notify(engine_time_t modified_time) {
        for (auto *subscriber: _subscribers) { subscriber->notify(modified_time); }
    }

    void BaseTimeSeriesOutput::_reset_last_modified_time() { _last_modified_time = MIN_DT; }

    // BaseTimeSeriesInput is now a template class with implementations in the header

} // namespace hgraph

