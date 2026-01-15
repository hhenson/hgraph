#include <fmt/format.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>

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

    // Legacy stub implementations - throw at runtime
    time_series_output_s_ptr OutputBuilder::make_instance(node_ptr /*owning_node*/) {
        throw std::runtime_error("OutputBuilder::make_instance(node_ptr) legacy method called - migrate to TSMeta-based API");
    }

    time_series_output_s_ptr OutputBuilder::make_instance(time_series_type_ptr /*owning_ts*/) {
        throw std::runtime_error("OutputBuilder::make_instance(time_series_type_ptr) legacy method called - migrate to TSMeta-based API");
    }

    void OutputBuilder::release_instance(time_series_output_s_ptr /*output*/) const {
        throw std::runtime_error("OutputBuilder::release_instance() legacy method called - no longer needed with TSValue");
    }

    void OutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<OutputBuilder, Builder>(m, "OutputBuilder")
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