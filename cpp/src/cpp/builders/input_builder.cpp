#include <fmt/format.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>

#include "hgraph/api/python/py_node.h"
#include <hgraph/builders/time_series_types/cpp_time_series_builder.h>
#include <hgraph/api/python/wrapper_factory.h>

namespace hgraph {
    void InputBuilder::release_instance(time_series_input_ptr item) const {
        // In the new system, time-series are value types owned by Node.
        // The builder doesn't own or cleanup time-series instances.
        // This method is kept for API compatibility but does nothing.
        (void)item;
    }

    void InputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<InputBuilder, Builder>(m, "InputBuilder")
                .def(
                    "make_instance",
                    [](InputBuilder::ptr self, nb::object owning_node,
                       nb::object owning_output) -> time_series_input_s_ptr {
                        if (!owning_node.is_none()) { return self->make_instance(unwrap_node(owning_node).get()); }
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

        // Unified Cpp time-series input builder (new system)
        cpp_time_series_input_builder_register_with_nanobind(m);
    }
} // namespace hgraph
