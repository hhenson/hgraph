#include <hgraph/types/base_time_series_input.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/node.h>

namespace hgraph {

    void BaseTimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<BaseTimeSeriesInput, TimeSeriesInput>(m, "BaseTimeSeriesInput");
    }

    TimeSeriesInput::ptr BaseTimeSeriesInput::parent_input() const {
        return static_cast<TimeSeriesInput *>(_parent_time_series().get());
    }

    bool BaseTimeSeriesInput::has_parent_input() const { return _has_parent_time_series(); }

    bool BaseTimeSeriesInput::bound() const { return _output != nullptr; }

    bool BaseTimeSeriesInput::has_peer() const { return _output != nullptr; }

    time_series_output_ptr BaseTimeSeriesInput::output() const { return _output; }

    bool BaseTimeSeriesInput::has_output() const { return _output.get() != nullptr; }

    bool BaseTimeSeriesInput::bind_output(time_series_output_ptr output_) {
        bool peer;
        bool was_bound = bound();

        if (auto ref_output = dynamic_cast<TimeSeriesReferenceOutput *>(output_.get())) {
            if (ref_output->valid() && ref_output->value()) { ref_output->value()->bind_input(*this); }
            ref_output->observe_reference(this);
            _reference_output = ref_output;
            peer = false;
        } else {
            if (output_ == _output) { return has_peer(); }
            peer = do_bind_output(output_);
        }

        if ((owning_node()->is_started() || owning_node()->is_starting()) && _output.get() && (was_bound || _output->valid())) {
            _sample_time = owning_graph()->evaluation_clock()->evaluation_time();
            if (active()) {
                notify(_sample_time);
            }
        }

        return peer;
    }

    void BaseTimeSeriesInput::un_bind_output(bool unbind_refs) {
        bool was_valid = valid();

        if (unbind_refs && _reference_output != nullptr) {
            _reference_output->stop_observing_reference(this);
            _reference_output.reset();
        }

        if (bound()) {
            do_un_bind_output(unbind_refs);

            if (owning_node()->is_started() && was_valid) {
                _sample_time = owning_graph()->evaluation_clock()->evaluation_time();
                if (active()) {
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
                    return;
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
        make_passive();
        _output = output_;
        if (active_) {
            make_active();
        }
        return true;
    }

    void BaseTimeSeriesInput::notify(engine_time_t modified_time) {
        if (_notify_time != modified_time) {
            _notify_time = modified_time;
            if (has_parent_input()) {
                parent_input()->notify_parent(this, modified_time);
            } else {
                owning_node()->notify(modified_time);
            }
        }
    }

    void BaseTimeSeriesInput::do_un_bind_output(bool) {
        if (_active) { output()->un_subscribe(this); }
        _output = nullptr;
    }

    void BaseTimeSeriesInput::builder_release_cleanup() {
        if (_output.get() != nullptr && _active) {
            _output->un_subscribe(this);
        }
        _active = false;
        if (_reference_output != nullptr) {
            _reference_output->stop_observing_reference(this);
            _reference_output.reset();
        }
        _output = nullptr;
    }

    void BaseTimeSeriesInput::notify_parent(TimeSeriesInput *, engine_time_t modified_time) {
        notify(modified_time);
    }

    void BaseTimeSeriesInput::set_sample_time(engine_time_t sample_time) { _sample_time = sample_time; }

    engine_time_t BaseTimeSeriesInput::sample_time() const { return _sample_time; }

    bool BaseTimeSeriesInput::sampled() const {
        return _sample_time != MIN_DT && _sample_time == owning_graph()->evaluation_clock()->evaluation_time();
    }

    time_series_reference_output_ptr BaseTimeSeriesInput::reference_output() const { return _reference_output; }

    const TimeSeriesInput *BaseTimeSeriesInput::get_input(size_t index) const {
        return const_cast<BaseTimeSeriesInput *>(this)->get_input(index);
    }

    TimeSeriesInput *BaseTimeSeriesInput::get_input(size_t) {
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

} // namespace hgraph
