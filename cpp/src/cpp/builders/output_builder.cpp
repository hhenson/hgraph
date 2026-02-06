#include <hgraph/builders/output_builder.h>
#include <hgraph/types/node.h>
#include <fmt/format.h>

#include "hgraph/api/python/py_node.h"

#include <hgraph/builders/ts_builders.h>
#include <hgraph/api/python/wrapper_factory.h>

namespace hgraph {
    void OutputBuilder::release_instance(time_series_output_ptr item) const {
        // Legacy cleanup methods removed with TSOutput migration.
        // TODO: Implement release via TSOutput's new API if needed.
    }

    void OutputBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_<OutputBuilder, Builder>(m, "OutputBuilder")
                .def(
                    "make_instance",
                    [](OutputBuilder::ptr self, nb::object owning_node,
                       nb::object owning_output) -> nb::object {
                        if (!owning_node.is_none()) {
                            auto node{unwrap_node(owning_node)};
                            auto output_{self->make_instance(node.get())};
                            auto view = output_->view(engine_time_t{});
                            return wrap_output_view(view);
                        }
                        if (!owning_output.is_none()) {
                            // Legacy unwrap_output removed; use view-based approach
                            auto &py_out = nb::cast<PyTimeSeriesOutput &>(owning_output);
                            auto output_{self->make_instance(py_out.output_view().output())};
                            auto view = output_->view(engine_time_t{});
                            return wrap_output_view(view);
                        }
                        // TODO: Here we need to create a standalone instance of the output

                        // Allow both to be None to match Python behavior
                        throw std::runtime_error("At least one of owning_node or owning_output must be provided");
                    },
                    "owning_node"_a = nb::none(), "owning_output"_a = nb::none())
                .def("release_instance", &OutputBuilder::release_instance)
                .def("__str__", [](const OutputBuilder &self) {
                    return fmt::format("OutputBuilder@{:p}", static_cast<const void *>(&self));
                })
                .def("__repr__", [](const OutputBuilder &self) {
                    return fmt::format("OutputBuilder@{:p}", static_cast<const void *>(&self));
                });

        // New TSValue-based output builder
        CppTimeSeriesOutputBuilder::register_with_nanobind(m);
    }
} // namespace hgraph
