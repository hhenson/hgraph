#include <fmt/format.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/api/python/python_api.h>

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
                       nb::object owning_output) -> nb::object {
                        time_series_input_ptr ts_input;
                        
                        if (!owning_node.is_none()) {
                            // Extract raw node from PyNode wrapper
                            node_ptr node;
                            if (auto* raw_node = hgraph::api::unwrap_node(owning_node)) {
                                node = node_ptr(raw_node);
                            } else {
                                node = nb::cast<node_ptr>(owning_node);
                            }
                            ts_input = self->make_instance(node);
                        } else if (!owning_output.is_none()) {
                            // Extract raw input from PyTimeSeriesInput wrapper
                            time_series_input_ptr parent_input;
                            if (auto* raw_input = hgraph::api::unwrap_input(owning_output)) {
                                parent_input = time_series_input_ptr(raw_input);
                            } else {
                                parent_input = nb::cast<time_series_input_ptr>(owning_output);
                            }
                            ts_input = self->make_instance(parent_input);
                        } else {
                            throw std::runtime_error("At least one of owning_node or owning_output must be provided");
                        }
                        
                        // Wrap with the owning graph's control block
                        return hgraph::api::wrap_input(ts_input.get(), ts_input->owning_graph()->api_control_block());
                    },
                    "owning_node"_a = nb::none(), "owning_output"_a = nb::none())
                .def("release_instance", 
                    [](const InputBuilder* self, nb::object ts_obj) {
                        // Extract raw time series from PyTimeSeriesInput wrapper
                        if (auto* raw_input = hgraph::api::unwrap_input(ts_obj)) {
                            time_series_input_ptr ts_input(raw_input);
                            self->release_instance(ts_input);
                        } else {
                            auto ts_input = nb::cast<time_series_input_ptr>(ts_obj);
                            self->release_instance(ts_input);
                        }
                    })
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