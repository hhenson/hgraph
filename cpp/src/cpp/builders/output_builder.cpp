#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>

// Include all the extracted builder headers
#include <hgraph/builders/time_series_types/time_series_value_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_ref_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_list_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_bundle_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_set_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_window_output_builder.h>
#include <hgraph/builders/time_series_types/time_series_dict_output_builder.h>

namespace hgraph {
    void OutputBuilder::release_instance(time_series_output_ptr item) const {
        // Perform minimal teardown before diagnostics to avoid dangling subscriptions
        item->builder_release_cleanup();
        // We can't check if we are in an error condition, nanobind should raise the python error into a C++
        // one, and then the state is gone, I think. Anyhow, if this is an issue, we can look into this later.
        if (item->_subscribers.size() != 0) {
            fmt::print("Output instance still has subscribers when released, this is a bug.\nOutput belongs to node: "
                       "{}\nSubscriber count: {}",
                       (item->has_owning_node() ? item->owning_node()->signature().name : "null"),
                       std::to_string(item->_subscribers.size()));
        }
        item->reset_parent_or_node();
    }

    void OutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<OutputBuilder, Builder>(m, "OutputBuilder")
                .def(
                    "make_instance",
                    [](OutputBuilder::ptr self, nb::object owning_node,
                       nb::object owning_output) -> time_series_output_ptr {
                        if (!owning_node.is_none()) { return self->make_instance(nb::cast<node_ptr>(owning_node)); }
                        if (!owning_output.is_none()) {
                            return self->make_instance(nb::cast<time_series_output_ptr>(owning_output));
                        }
                        // Allow both to be None to match Python behavior
                        return self->make_instance(node_ptr(nullptr));
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
        TimeSeriesRefOutputBuilder::register_with_nanobind(m);
        TimeSeriesListOutputBuilder::register_with_nanobind(m);
        TimeSeriesBundleOutputBuilder::register_with_nanobind(m);
        time_series_set_output_builder_register_with_nanobind(m);
        time_series_window_output_builder_register_with_nanobind(m);
        time_series_dict_output_builder_register_with_nanobind(m);
    }
} // namespace hgraph