#include <fmt/format.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

// Include all the extracted builder headers
#include "hgraph/api/python/py_node.h"

#include <hgraph/builders/time_series_types/specialized_ref_builders.h>
#include <hgraph/builders/time_series_types/time_series_bundle_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_dict_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_list_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_set_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_signal_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_value_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_window_input_builder.h>
#include <hgraph/api/python/wrapper_factory.h>

namespace hgraph {

    // Legacy stub implementations - throw at runtime
    time_series_input_s_ptr InputBuilder::make_instance(node_ptr /*owning_node*/) {
        throw std::runtime_error("InputBuilder::make_instance(node_ptr) legacy method called - migrate to TSMeta-based API");
    }

    time_series_input_s_ptr InputBuilder::make_instance(time_series_type_ptr /*owning_ts*/) {
        throw std::runtime_error("InputBuilder::make_instance(time_series_type_ptr) legacy method called - migrate to TSMeta-based API");
    }

    void InputBuilder::release_instance(time_series_input_s_ptr /*input*/) const {
        throw std::runtime_error("InputBuilder::release_instance() legacy method called - no longer needed with TSValue");
    }

    void InputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<InputBuilder, Builder>(m, "InputBuilder")
                .def("__str__", [](const InputBuilder &self) {
                    return fmt::format("InputBuilder@{:p}", static_cast<const void *>(&self));
                })
                .def("__repr__", [](const InputBuilder &self) {
                    return fmt::format("InputBuilder@{:p}", static_cast<const void *>(&self));
                });

        // Call the register functions from each builder type
        TimeSeriesSignalInputBuilder::register_with_nanobind(m);
        time_series_value_input_builder_register_with_nanobind(m);

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