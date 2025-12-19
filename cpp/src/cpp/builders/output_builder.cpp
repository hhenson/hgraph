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
#include <hgraph/builders/time_series_types/cpp_time_series_builder.h>
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
                    [](OutputBuilder::ptr, nb::object, nb::object) -> nb::object {
                        // Builders don't create time-series instances.
                        // Time-series are value types owned by the Node, created via emplace methods.
                        throw std::runtime_error("OutputBuilder.make_instance not supported");
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

        // Unified Cpp time-series output builder
        cpp_time_series_output_builder_register_with_nanobind(m);
    }
} // namespace hgraph