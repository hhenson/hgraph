//
// BaseTimeSeriesInput template method implementations
//
// This file should be included AFTER ref.h to avoid circular dependency issues.
// It contains the template method implementations that need TimeSeriesReferenceOutput to be fully defined.
//

#ifndef BASE_TIME_SERIES_IMPL_H
#define BASE_TIME_SERIES_IMPL_H

#include <hgraph/types/base_time_series.h>
#include <hgraph/types/ref.h>

namespace hgraph {

    // Template method implementations that need full type info
    template<typename T>
    requires std::is_base_of_v<TimeSeriesInput, T>
    bool BaseTimeSeriesInput<T>::bind_output(const time_series_output_s_ptr &output_) {
        bool peer;
        bool was_bound = bound();

        if (auto ref_output = std::dynamic_pointer_cast<TimeSeriesReferenceOutput>(output_)) {
            if (ref_output->valid() && ref_output->has_value()) { ref_output->value().bind_input(*this); }
            ref_output->observe_reference(this);
            _reference_output = ref_output;
            peer = false;
        } else if (auto v2_ref_output = std::dynamic_pointer_cast<TimeSeriesValueReferenceOutput>(output_)) {
            // v2 style reference output - get the TimeSeriesReference and bind through it
            auto ref_opt = v2_ref_output->reference_value();
            if (ref_opt && ref_opt->has_output()) {
                // Bind directly to the underlying output that the reference points to
                do_bind_output(ref_opt->output());
            }
            // Register as observer to be notified when the reference value changes
            v2_ref_output->observe_reference(this);
            _v2_reference_output = v2_ref_output;  // Store to clean up later
            peer = false;
        } else {
            if (output_.get() == _output.get()) { return has_peer(); }
            peer = do_bind_output(output_);
        }

        if (has_owning_node()) {
            auto n = owning_node();
            if ((n->is_started() || n->is_starting()) && _output && (was_bound || _output->valid())) {
                _sample_time = *n->cached_evaluation_time_ptr();
                if (active()) {
                    notify(_sample_time);
                }
            }
        }

        return peer;
    }

    template<typename T>
    requires std::is_base_of_v<TimeSeriesInput, T>
    void BaseTimeSeriesInput<T>::un_bind_output(bool unbind_refs) {
        bool was_valid = valid();

        if (unbind_refs && _reference_output != nullptr) {
            _reference_output->stop_observing_reference(this);
            _reference_output = nullptr;
        }

        if (unbind_refs && _v2_reference_output != nullptr) {
            _v2_reference_output->stop_observing_reference(this);
            _v2_reference_output = nullptr;
        }

        if (bound()) {
            do_un_bind_output(unbind_refs);

            if (has_owning_node()) {
                auto n = owning_node();
                if (n->is_started() && was_valid) {
                    _sample_time = *n->cached_evaluation_time_ptr();
                    if (active()) {
                        n->notify(_sample_time);
                    }
                }
            }
        }
    }

    template<typename T>
    requires std::is_base_of_v<TimeSeriesInput, T>
    void BaseTimeSeriesInput<T>::make_active() {
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

    template<typename T>
    requires std::is_base_of_v<TimeSeriesInput, T>
    void BaseTimeSeriesInput<T>::make_passive() {
        if (_active) {
            _active = false;
            if (_output != nullptr) { output()->un_subscribe(this); }
        }
    }

    template<typename T>
    requires std::is_base_of_v<TimeSeriesInput, T>
    bool BaseTimeSeriesInput<T>::do_bind_output(time_series_output_s_ptr output_) {
        if (_active && _output != nullptr) {
            _output->un_subscribe(this);
        }
        _output = output_;
        if (_active && output_) {
            output_->subscribe(this);
        }
        return true;
    }

    template<typename T>
    requires std::is_base_of_v<TimeSeriesInput, T>
    void BaseTimeSeriesInput<T>::do_un_bind_output(bool unbind_refs) {
        if (_active && _output != nullptr) {
            _output->un_subscribe(this);
        }
        _output = nullptr;
    }

    template<typename T>
    requires std::is_base_of_v<TimeSeriesInput, T>
    void BaseTimeSeriesInput<T>::notify(engine_time_t modified_time) {
        if (_notify_time == modified_time) { return; }
        _notify_time = modified_time;
        if (_has_parent_input()) {
            _parent_input()->notify_parent(this, modified_time);
        } else if (has_owning_node()) {
            owning_node()->notify(modified_time);
        }
        // If no parent input and no owning node, silently ignore - input is not yet wired
    }

} // namespace hgraph

#endif  // BASE_TIME_SERIES_IMPL_H
