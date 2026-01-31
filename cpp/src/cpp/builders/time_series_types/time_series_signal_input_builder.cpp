#include <hgraph/builders/time_series_types/time_series_signal_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

namespace hgraph {
    TimeSeriesSignalInputBuilder::TimeSeriesSignalInputBuilder(InputBuilder::ptr impl_builder)
        : impl_builder_{std::move(impl_builder)} {}

    time_series_input_s_ptr TimeSeriesSignalInputBuilder::make_instance(node_ptr owning_node) const {
        auto sig = arena_make_shared_as<TimeSeriesSignalInput, TimeSeriesInput>(owning_node);
        if (impl_builder_) {
            // Create the impl input with the signal as its parent, matching Python behavior
            auto signal_ptr = static_cast<TimeSeriesSignalInput*>(sig.get());
            signal_ptr->_impl = impl_builder_->make_instance(signal_ptr);
        }
        return sig;
    }

    time_series_input_s_ptr TimeSeriesSignalInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        auto sig = arena_make_shared_as<TimeSeriesSignalInput, TimeSeriesInput>(owning_input);
        if (impl_builder_) {
            // Create the impl input with the signal as its parent, matching Python behavior
            auto signal_ptr = static_cast<TimeSeriesSignalInput*>(sig.get());
            signal_ptr->_impl = impl_builder_->make_instance(signal_ptr);
        }
        return sig;
    }

    void TimeSeriesSignalInputBuilder::release_instance(time_series_input_ptr item) const {
        release_instance(dynamic_cast<TimeSeriesSignalInput *>(item));
    }

    void TimeSeriesSignalInputBuilder::release_instance(TimeSeriesSignalInput *signal_input) const {
        if (signal_input == nullptr) {
            throw std::runtime_error("TimeSeriesSignalInputBuilder::release_instance: expected TimeSeriesSignalInput but got different type");
        }
        InputBuilder::release_instance(signal_input);
        // Release impl if present
        if (signal_input->_impl && impl_builder_) {
            impl_builder_->release_instance(signal_input->_impl.get());
        }
        if (signal_input->_ts_values.empty()) { return; }
        for (auto &ts_value: signal_input->_ts_values) { release_instance(ts_value.get()); }
    }

    size_t TimeSeriesSignalInputBuilder::memory_size() const {
        size_t total = add_canary_size(sizeof(TimeSeriesSignalInput));
        if (impl_builder_) {
            total = align_size(total, impl_builder_->type_alignment());
            total += impl_builder_->memory_size();
        }
        return total;
    }

    size_t TimeSeriesSignalInputBuilder::type_alignment() const {
        return alignof(TimeSeriesSignalInput);
    }

    void TimeSeriesSignalInputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesSignalInputBuilder, InputBuilder>(m, "InputBuilder_TS_Signal")
            .def(nb::init<>())
            .def(nb::init<InputBuilder::ptr>(), "impl_builder"_a);
    }
} // namespace hgraph
