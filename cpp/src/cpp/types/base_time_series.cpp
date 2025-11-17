#include <hgraph/types/base_time_series.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>

namespace hgraph {

    // ============================================================================
    // BaseTimeSeriesOutput Implementation
    // ============================================================================

    // Implement TimeSeriesType pure virtuals for Output
    node_ptr BaseTimeSeriesOutput::owning_node() { return _owning_node(); }
    node_ptr BaseTimeSeriesOutput::owning_node() const { return _owning_node(); }

    graph_ptr BaseTimeSeriesOutput::owning_graph() {
        return has_owning_node() ? owning_node()->graph() : graph_ptr{};
    }

    graph_ptr BaseTimeSeriesOutput::owning_graph() const {
        return has_owning_node() ? owning_node()->graph() : graph_ptr{};
    }

    bool BaseTimeSeriesOutput::is_reference() const { return false; }
    bool BaseTimeSeriesOutput::has_reference() const { return false; }

    void BaseTimeSeriesOutput::reset_parent_or_node() { _parent_ts_or_node.reset(); }
    
    // Implement re_parent methods
    void BaseTimeSeriesOutput::re_parent(const node_ptr &parent) {
        _parent_ts_or_node = parent;
    }
    void BaseTimeSeriesOutput::re_parent(const TimeSeriesType::ptr &parent) {
        _parent_ts_or_node = parent;
    }

    // TimeSeriesType helper methods
    TimeSeriesType::ptr &BaseTimeSeriesOutput::_parent_time_series() const {
        return const_cast<BaseTimeSeriesOutput *>(this)->_parent_time_series();
    }

    TimeSeriesType::ptr &BaseTimeSeriesOutput::_parent_time_series() {
        if (_parent_ts_or_node.has_value() && std::holds_alternative<TimeSeriesType::ptr>(*_parent_ts_or_node)) {
            return std::get<TimeSeriesType::ptr>(*_parent_ts_or_node);
        } else {
            return TimeSeriesType::null_ptr;
        }
    }

    bool BaseTimeSeriesOutput::_has_parent_time_series() const {
        return _parent_ts_or_node.has_value() && std::holds_alternative<TimeSeriesType::ptr>(*_parent_ts_or_node);
    }

    void BaseTimeSeriesOutput::_set_parent_time_series(TimeSeriesType *ts) {
        if (_parent_ts_or_node.has_value() && std::holds_alternative<TimeSeriesType::ptr>(*_parent_ts_or_node)) {
            std::get<TimeSeriesType::ptr>(*_parent_ts_or_node) = ts;
        } else {
            _parent_ts_or_node = TimeSeriesType::ptr{ts};
        }
    }

    bool BaseTimeSeriesOutput::has_parent_or_node() const { return _parent_ts_or_node.has_value(); }

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

    node_ptr BaseTimeSeriesOutput::_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            return std::visit(
                []<typename T_>(T_ &&value) -> node_ptr {
                    using T = std::decay_t<T_>;
                    if constexpr (std::is_same_v<T, TimeSeriesType::ptr>) {
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

    void BaseTimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        // Register just for type hierarchy - methods are defined on interface
        nb::class_<BaseTimeSeriesOutput, TimeSeriesOutput>(m, "BaseTimeSeriesOutput");
    }

    TimeSeriesOutput::ptr BaseTimeSeriesOutput::parent_output() const {
        return static_cast<TimeSeriesOutput *>(_parent_time_series().get()); // NOLINT(*-pro-type-static-cast-downcast)
    }

    TimeSeriesOutput::ptr BaseTimeSeriesOutput::parent_output() {
        return static_cast<TimeSeriesOutput *>(_parent_time_series().get()); // NOLINT(*-pro-type-static-cast-downcast)
    }

    bool BaseTimeSeriesOutput::has_parent_output() const { return _has_parent_time_series(); }

    bool BaseTimeSeriesOutput::can_apply_result(nb::object value) {
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
        if (n.get() == nullptr) { return false; }
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
            if (n.get() != nullptr) {
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
            if (n.get() != nullptr) {
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

    void BaseTimeSeriesOutput::un_subscribe(Notifiable *notifiable) { _subscribers.erase(notifiable); }

    void BaseTimeSeriesOutput::_notify(engine_time_t modified_time) {
        for (auto *subscriber: _subscribers) { subscriber->notify(modified_time); }
    }

    void BaseTimeSeriesOutput::_reset_last_modified_time() { _last_modified_time = MIN_DT; }

    // ============================================================================
    // BaseTimeSeriesInput Implementation
    // ============================================================================

    // Implement TimeSeriesType pure virtuals for Input
    node_ptr BaseTimeSeriesInput::owning_node() { return _owning_node(); }
    node_ptr BaseTimeSeriesInput::owning_node() const { return _owning_node(); }

    graph_ptr BaseTimeSeriesInput::owning_graph() {
        return has_owning_node() ? owning_node()->graph() : graph_ptr{};
    }

    graph_ptr BaseTimeSeriesInput::owning_graph() const {
        return has_owning_node() ? owning_node()->graph() : graph_ptr{};
    }

    bool BaseTimeSeriesInput::is_reference() const { return false; }
    bool BaseTimeSeriesInput::has_reference() const { return false; }

    void BaseTimeSeriesInput::reset_parent_or_node() { _parent_ts_or_node.reset(); }
    
    // Implement re_parent methods
    void BaseTimeSeriesInput::re_parent(const node_ptr &parent) {
        _parent_ts_or_node = parent;
    }
    void BaseTimeSeriesInput::re_parent(const TimeSeriesType::ptr &parent) {
        _parent_ts_or_node = parent;
    }

    // TimeSeriesType helper methods
    TimeSeriesType::ptr &BaseTimeSeriesInput::_parent_time_series() const {
        return const_cast<BaseTimeSeriesInput *>(this)->_parent_time_series();
    }

    TimeSeriesType::ptr &BaseTimeSeriesInput::_parent_time_series() {
        if (_parent_ts_or_node.has_value() && std::holds_alternative<TimeSeriesType::ptr>(*_parent_ts_or_node)) {
            return std::get<TimeSeriesType::ptr>(*_parent_ts_or_node);
        } else {
            return TimeSeriesType::null_ptr;
        }
    }

    bool BaseTimeSeriesInput::_has_parent_time_series() const {
        return _parent_ts_or_node.has_value() && std::holds_alternative<TimeSeriesType::ptr>(*_parent_ts_or_node);
    }

    void BaseTimeSeriesInput::_set_parent_time_series(TimeSeriesType *ts) {
        if (_parent_ts_or_node.has_value() && std::holds_alternative<TimeSeriesType::ptr>(*_parent_ts_or_node)) {
            std::get<TimeSeriesType::ptr>(*_parent_ts_or_node) = ts;
        } else {
            _parent_ts_or_node = TimeSeriesType::ptr{ts};
        }
    }

    bool BaseTimeSeriesInput::has_parent_or_node() const { return _parent_ts_or_node.has_value(); }

    bool BaseTimeSeriesInput::has_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            if (std::holds_alternative<node_ptr>(*_parent_ts_or_node)) {
                return std::get<node_ptr>(*_parent_ts_or_node) != node_ptr{};
            }
            return std::get<TimeSeriesType::ptr>(*_parent_ts_or_node)->has_owning_node();
        } else {
            return false;
        }
    }

    node_ptr BaseTimeSeriesInput::_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            return std::visit(
                []<typename T_>(T_ &&value) -> node_ptr {
                    using T = std::decay_t<T_>;
                    if constexpr (std::is_same_v<T, TimeSeriesType::ptr>) {
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

    TimeSeriesInput::ptr BaseTimeSeriesInput::parent_input() const {
        return static_cast<TimeSeriesInput *>(_parent_time_series().get()); // NOLINT(*-pro-type-static-cast-downcast)
    }

    bool BaseTimeSeriesInput::has_parent_input() const { return _has_parent_time_series(); }

    bool BaseTimeSeriesInput::bound() const { return _output != nullptr; }

    bool BaseTimeSeriesInput::has_peer() const {
        // By default, we assume that if there is an output, then we are peered.
        // This is not always True but is a good general assumption.
        return _output != nullptr;
    }

    time_series_output_ptr BaseTimeSeriesInput::output() const { return _output; }

    bool BaseTimeSeriesInput::has_output() const { return _output.get() != nullptr; }

    bool BaseTimeSeriesInput::bind_output(time_series_output_ptr output_) {
        bool peer;
        bool was_bound = bound(); // Track if input was previously bound (matches Python behavior)

        if (auto ref_output = dynamic_cast<TimeSeriesReferenceOutput *>(output_.get())) {
            // Is a TimeseriesReferenceOutput
            // Match Python behavior: only check if value exists (truthy), bind if it does
            if (ref_output->valid() && ref_output->has_value()) { ref_output->value().bind_input(*this); }
            ref_output->observe_reference(this);
            _reference_output = ref_output;
            peer = false;
        } else {
            if (output_ == _output) { return has_peer(); }
            peer = do_bind_output(output_);
        }

        // Notify if the node is started/starting and either:
        // - The input was previously bound (rebinding case), OR
        // - The new output is valid
        // This matches the Python implementation: (was_bound or self._output.valid)
        auto n = owning_node();
        if ((n->is_started() || n->is_starting()) && _output.get() && (was_bound || _output->valid())) {
            // Use cached evaluation time pointer from node for performance
            _sample_time = *n->cached_evaluation_time_ptr();
            if (active()) {
                notify(_sample_time);
                // TODO: This might belong to make_active, or not? There is a race with setting sample_time too.
            }
        }

        return peer;
    }

    void BaseTimeSeriesInput::un_bind_output(bool unbind_refs) {
        bool was_valid = valid();

        // Handle reference output unbinding conditionally based on unbind_refs parameter
        if (unbind_refs && _reference_output != nullptr) {
            _reference_output->stop_observing_reference(this);
            _reference_output.reset();
        }

        if (bound()) {
            do_un_bind_output(unbind_refs);

            auto n = owning_node();
            if (n->is_started() && was_valid) {
                // Use cached evaluation time pointer from node for performance
                _sample_time = *n->cached_evaluation_time_ptr();
                if (active()) {
                    // Notify as the state of the node has changed from bound to un_bound
                    n->notify(_sample_time);
                }
            }
        }
    }

    bool BaseTimeSeriesInput::active() const { return _active; }

    void BaseTimeSeriesInput::make_active() {
        if (!_active) {
            _active = true;
            if (_output != nullptr) {
                output()->subscribe(this);
                if (output()->valid() && output()->modified()) {
                    notify(output()->last_modified_time());
                    return; // If the output is modified, we do not need to check if sampled
                }
            }

            if (sampled()) { notify(_sample_time); }
        }
    }

    void BaseTimeSeriesInput::make_passive() {
        if (_active) {
            _active = false;
            if (_output != nullptr) { output()->un_subscribe(this); }
        }
    }

    nb::object BaseTimeSeriesInput::py_value() const {
        if (_output != nullptr) {
            return _output->py_value();
        } else {
            return nb::none();
        }
    }

    nb::object BaseTimeSeriesInput::py_delta_value() const {
        if (_output != nullptr) {
            return _output->py_delta_value();
        } else {
            return nb::none();
        }
    }

    void BaseTimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        // Register just for type hierarchy - methods are defined on interface
        nb::class_<BaseTimeSeriesInput, TimeSeriesInput>(m, "BaseTimeSeriesInput");
    }

    bool BaseTimeSeriesInput::do_bind_output(time_series_output_ptr &output_) {
        auto active_{active()};
        make_passive(); // Ensure we are unsubscribed from the old output.
        _output = output_;
        if (active_) {
            make_active(); // If we were active now subscribe to the new output,
            // this is important even if we were not bound previously as this will ensure the new output gets
            // subscribed to
        }
        return true;
    }

    auto BaseTimeSeriesInput::notify(engine_time_t modified_time) -> void { // NOLINT(*-no-recursion)
        if (_notify_time != modified_time) {
            _notify_time = modified_time;
            if (has_parent_input()) {
                // Cast to BaseTimeSeriesInput to access protected notify_parent
                auto parent = static_cast<BaseTimeSeriesInput*>(parent_input().get());
                parent->notify_parent(this, modified_time);
            } else {
                owning_node()->notify(modified_time);
            }
        }
    }

    void BaseTimeSeriesInput::do_un_bind_output(bool unbind_refs) {
        if (_active) { output()->un_subscribe(this); }
        _output = nullptr;
    }

    // Minimal-teardown helper: avoid consulting owning_node/graph
    void BaseTimeSeriesInput::builder_release_cleanup() {
        if (_output.get() != nullptr && _active) {
            // Unsubscribe from output without triggering any node notifications
            _output->un_subscribe(this);
        }
        _active = false;
        if (_reference_output != nullptr) {
            _reference_output->stop_observing_reference(this);
            _reference_output.reset();
        }
        _output = nullptr;
    }

    void BaseTimeSeriesInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        notify(modified_time);
    } // NOLINT(*-no-recursion)

    void BaseTimeSeriesInput::set_sample_time(engine_time_t sample_time) { _sample_time = sample_time; }

    engine_time_t BaseTimeSeriesInput::sample_time() const { return _sample_time; }

    bool BaseTimeSeriesInput::sampled() const {
        auto n = owning_node();
        if (n.get() == nullptr) { return false; }
        // Use cached evaluation time pointer from node for performance
        return _sample_time != MIN_DT && _sample_time == *n->cached_evaluation_time_ptr();
    }

    time_series_reference_output_ptr BaseTimeSeriesInput::reference_output() const { return _reference_output; }

    const TimeSeriesInput *BaseTimeSeriesInput::get_input(size_t index) const {
        return const_cast<BaseTimeSeriesInput *>(this)->get_input(index);
    }

    TimeSeriesInput *BaseTimeSeriesInput::get_input(size_t index) {
        throw std::runtime_error("BaseTimeSeriesInput [] not supported");
    }

    void BaseTimeSeriesInput::reset_output() { _output = nullptr; }

    void BaseTimeSeriesInput::set_output(time_series_output_ptr output) { _output = std::move(output); }

    void BaseTimeSeriesInput::set_active(bool active) { _active = active; }

    bool BaseTimeSeriesInput::modified() const { return _output != nullptr && (_output->modified() || sampled()); }

    bool BaseTimeSeriesInput::valid() const { return bound() && _output != nullptr && _output->valid(); }

    bool BaseTimeSeriesInput::all_valid() const { return bound() && _output != nullptr && _output->all_valid(); }

    engine_time_t BaseTimeSeriesInput::last_modified_time() const {
        return bound() ? std::max(_output->last_modified_time(), _sample_time) : MIN_DT;
    }
} // namespace hgraph

