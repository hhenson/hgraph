#include <hgraph/builders/output_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/api/python/python_api.h>

// Include all the extracted builder headers
#include <hgraph/builders/time_series_types/time_series_value_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_ref_output_builder.h>
#include <hgraph/builders/time_series_types/specialized_ref_builders.h>
#include <hgraph/builders/time_series_types/time_series_list_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_bundle_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_set_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_window_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_dict_output_builder.h>

namespace hgraph {
    void OutputBuilder::release_instance(time_series_output_ptr item) const {
        // Perform minimal teardown - builder_release_cleanup handles subscriber cleanup
        item->builder_release_cleanup();
        item->reset_parent_or_node();
    }

    void OutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<OutputBuilder, Builder>(m, "OutputBuilder")
                .def(
                    "make_instance",
                    [](OutputBuilder::ptr self, nb::object owning_node,
                       nb::object owning_output) -> nb::object {
                        time_series_output_ptr ts_output;
                        
                        if (!owning_node.is_none()) {
                            // Extract raw node from PyNode wrapper
                            node_ptr node;
                            if (auto* raw_node = hgraph::api::unwrap_node(owning_node)) {
                                node = node_ptr(raw_node);
                            } else {
                                node = nb::cast<node_ptr>(owning_node);
                            }
                            ts_output = self->make_instance(node);
                        } else if (!owning_output.is_none()) {
                            // Extract raw output from PyTimeSeriesOutput wrapper
                            time_series_output_ptr parent_output;
                            if (auto* raw_output = hgraph::api::unwrap_output(owning_output)) {
                                parent_output = time_series_output_ptr(raw_output);
                            } else {
                                parent_output = nb::cast<time_series_output_ptr>(owning_output);
                            }
                            ts_output = self->make_instance(parent_output);
                        } else {
                            // Allow both to be None to match Python behavior
                            ts_output = self->make_instance(node_ptr(nullptr));
                        }
                        
                        // Wrap with the owning graph's control block (if available)
                        if (ts_output && ts_output->owning_graph()) {
                            return hgraph::api::wrap_output(ts_output.get(), ts_output->owning_graph()->api_control_block());
                        }
                        // If no owning graph, create a temporary control block (this shouldn't normally happen)
                        return hgraph::api::wrap_output(ts_output.get(), std::make_shared<hgraph::api::ApiControlBlock>());
                    },
                    "owning_node"_a = nb::none(), "owning_output"_a = nb::none())
                .def("release_instance", 
                    [](const OutputBuilder* self, nb::object ts_obj) {
                        // Extract raw time series from PyTimeSeriesOutput wrapper
                        if (auto* raw_output = hgraph::api::unwrap_output(ts_obj)) {
                            time_series_output_ptr ts_output(raw_output);
                            self->release_instance(ts_output);
                        } else {
                            auto ts_output = nb::cast<time_series_output_ptr>(ts_obj);
                            self->release_instance(ts_output);
                        }
                    })
                .def("__str__", [](const OutputBuilder &self) {
                    return fmt::format("OutputBuilder@{:p}", static_cast<const void *>(&self));
                })
                .def("__repr__", [](const OutputBuilder &self) {
                    return fmt::format("OutputBuilder@{:p}", static_cast<const void *>(&self));
                });

        // Call the register functions from each builder type
        time_series_value_output_builder_register_with_nanobind(m);
        TimeSeriesRefOutputBuilder::register_with_nanobind(m);
        
        // Specialized reference output builders
        TimeSeriesValueRefOutputBuilder::register_with_nanobind(m);
        TimeSeriesListRefOutputBuilder::register_with_nanobind(m);
        TimeSeriesBundleRefOutputBuilder::register_with_nanobind(m);
        TimeSeriesDictRefOutputBuilder::register_with_nanobind(m);
        TimeSeriesSetRefOutputBuilder::register_with_nanobind(m);
        TimeSeriesWindowRefOutputBuilder::register_with_nanobind(m);
        
        TimeSeriesListOutputBuilder::register_with_nanobind(m);
        TimeSeriesBundleOutputBuilder::register_with_nanobind(m);
        time_series_set_output_builder_register_with_nanobind(m);
        time_series_window_output_builder_register_with_nanobind(m);
        time_series_dict_output_builder_register_with_nanobind(m);
    }
} // namespace hgraph