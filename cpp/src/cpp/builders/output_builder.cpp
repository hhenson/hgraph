#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series_type.h>

#include "hgraph/api/python/py_node.h"
#include <hgraph/builders/time_series_types/cpp_time_series_builder.h>
#include <hgraph/api/python/wrapper_factory.h>

namespace hgraph {
    void OutputBuilder::release_instance(time_series_output_ptr item) const {
        // In the new system, time-series are value types owned by Node.
        // The builder doesn't own or cleanup time-series instances.
        // This method is kept for API compatibility but does nothing.
        (void)item;
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

        // Unified Cpp time-series output builder (new system)
        cpp_time_series_output_builder_register_with_nanobind(m);
    }
} // namespace hgraph
