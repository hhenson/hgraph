#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>

// Include all the extracted builder headers
#include "hgraph/api/python/py_node.h"

#include <hgraph/builders/time_series_types/specialized_ref_builders.h>
#include <hgraph/builders/time_series_types/time_series_bundle_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_dict_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_list_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_set_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_value_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_window_output_builder.h>
#include <hgraph/api/python/wrapper_factory.h>

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
                        if (!owning_node.is_none()) {
                            node_ptr node_shared = unwrap_node(owning_node);
                            if (!node_shared) { throw std::runtime_error("Invalid node"); }
                            // Python bindings only support heap allocation (no buffer/offset)
                            auto output_{self->make_instance(node_shared, nullptr, nullptr)} ;
                            return wrap_time_series(output_);
                        }
                        if (!owning_output.is_none()) {
                            time_series_output_ptr output_shared = unwrap_output(owning_output);
                            if (!output_shared) { throw std::runtime_error("Invalid output"); }
                            // Python bindings only support heap allocation (no buffer/offset)
                            auto result_{self->make_instance(output_shared, nullptr, nullptr)};
                            return wrap_time_series(result_);
                        }
                        // Create an output with a null node_ptr
                        node_ptr node_shared = nullptr;
                        auto result_{self->make_instance(node_shared, nullptr, nullptr)};
                        return wrap_time_series(result_);
                    },
                    "owning_node"_a = nb::none(), "owning_output"_a = nb::none())
                .def("release_instance", &OutputBuilder::release_instance)
                .def("__str__", [](const OutputBuilder &self) {
                    return fmt::format("OutputBuilder@{:p}", static_cast<const void *>(&self));
                })
                .def("__repr__", [](const OutputBuilder &self) {
                    return fmt::format("OutputBuilder@{:p}", static_cast<const void *>(&self));
                });

        // Call the register functions from each builder type
        time_series_value_output_builder_register_with_nanobind(m);
        
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