#include <fmt/format.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>

// Include all the extracted builder headers
#include <hgraph/builders/time_series_types/time_series_signal_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_value_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_ref_input_builder.h>
#include <hgraph/builders/time_series_types/specialized_ref_builders.h>
#include <hgraph/builders/time_series_types/time_series_list_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_bundle_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_set_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_window_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_dict_input_builder.h>

namespace hgraph {
    void InputBuilder::release_instance(time_series_input_ptr item) const {
        // Perform minimal teardown to avoid notifications and dangling refs
        item->builder_release_cleanup();
        // We can't detect if we are escaping from an error condition or not, so just log issues.
        if (item->has_output()) {
            fmt::print("Input instance still has an output reference when released, this is a bug.");
        }
        item->reset_parent_or_node();
    }

    void InputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<InputBuilder, Builder>(m, "InputBuilder")
                .def(
                    "make_instance",
                    [](InputBuilder::ptr self, nb::object owning_node,
                       nb::object owning_output) -> time_series_input_ptr {
                        if (!owning_node.is_none()) { return self->make_instance(nb::cast<node_ptr>(owning_node)); }
                        if (!owning_output.is_none()) {
                            return self->make_instance(nb::cast<time_series_input_ptr>(owning_output));
                        }
                        throw std::runtime_error("At least one of owning_node or owning_output must be provided");
                    },
                    "owning_node"_a = nb::none(), "owning_output"_a = nb::none())
                .def("release_instance", &InputBuilder::release_instance)
                .def("__str__", [](const InputBuilder &self) {
                    return fmt::format("InputBuilder@{:p}", static_cast<const void *>(&self));
                })
                .def("__repr__", [](const InputBuilder &self) {
                    return fmt::format("InputBuilder@{:p}", static_cast<const void *>(&self));
                });

        // Call the register functions from each builder type
        TimeSeriesSignalInputBuilder::register_with_nanobind(m);
        time_series_value_input_builder_register_with_nanobind(m);
        TimeSeriesRefInputBuilder::register_with_nanobind(m);
        
        // Specialized reference input builders
        TimeSeriesValueRefInputBuilder::register_with_nanobind(m);
        TimeSeriesListRefInputBuilder::register_with_nanobind(m);
        TimeSeriesBundleRefInputBuilder::register_with_nanobind(m);
        TimeSeriesDictRefInputBuilder::register_with_nanobind(m);
        TimeSeriesSetRefInputBuilder::register_with_nanobind(m);
        TimeSeriesWindowRefInputBuilder::register_with_nanobind(m);
        
        TimeSeriesListInputBuilder::register_with_nanobind(m);
        TimeSeriesBundleInputBuilder::register_with_nanobind(m);
        time_series_set_input_builder_register_with_nanobind(m);
        time_series_window_input_builder_register_with_nanobind(m);
        time_series_dict_input_builder_register_with_nanobind(m);
    }
} // namespace hgraph