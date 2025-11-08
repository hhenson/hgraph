#include <hgraph/types/base_time_series_input.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

namespace hgraph {

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
            if (ref_output->valid() && ref_output->value()) { ref_output->value()->bind_input(*this); }
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
        if ((owning_node()->is_started() || owning_node()->is_starting()) && _output.get() && (was_bound || _output->
                valid())) {
            _sample_time = owning_graph()->evaluation_clock()->evaluation_time();
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

            if (owning_node()->is_started() && was_valid) {
                _sample_time = owning_graph()->evaluation_clock()->evaluation_time();
                if (active()) {
                    // Notify as the state of the node has changed from bound to un_bound
                    owning_node()->notify(_sample_time);
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
                dynamic_cast<BaseTimeSeriesInput*>(parent_input().get())->notify_parent(this, modified_time);
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
        return _sample_time != MIN_DT && _sample_time == owning_graph()->evaluation_clock()->evaluation_time();
    }

    time_series_reference_output_ptr BaseTimeSeriesInput::reference_output() const { return _reference_output; }

    const TimeSeriesInput *BaseTimeSeriesInput::get_input(size_t index) const {
        return const_cast<BaseTimeSeriesInput *>(this)->get_input(index);
    }

    TimeSeriesInput *BaseTimeSeriesInput::get_input(size_t index) {
        throw std::runtime_error("TimeSeriesInput [] not supported");
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

    void BaseTimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<BaseTimeSeriesInput, TimeSeriesInput>(m, "BaseTimeSeriesInput");
    }

} // namespace hgraph
