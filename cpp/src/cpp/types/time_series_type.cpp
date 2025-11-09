#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>

namespace hgraph {
    void TimeSeriesType::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesType, nb::intrusive_base>(m, "TimeSeriesType")
                .def_prop_ro("owning_node",
                             static_cast<node_ptr (TimeSeriesType::*)() const>(&TimeSeriesType::owning_node))
                .def_prop_ro("owning_graph",
                             static_cast<graph_ptr (TimeSeriesType::*)() const>(&TimeSeriesType::owning_graph))
                .def_prop_ro("value", &TimeSeriesType::py_value)
                .def_prop_ro("delta_value", &TimeSeriesType::py_delta_value)
                .def_prop_ro("modified", &TimeSeriesType::modified)
                .def_prop_ro("valid", &TimeSeriesType::valid)
                .def_prop_ro("all_valid", &TimeSeriesType::all_valid)
                .def_prop_ro("last_modified_time", &TimeSeriesType::last_modified_time)
                .def("re_parent", static_cast<void (TimeSeriesType::*)(const Node::ptr &)>(&TimeSeriesType::re_parent))
                .def("re_parent", static_cast<void (TimeSeriesType::*)(const ptr &)>(&TimeSeriesType::re_parent))
                .def("is_reference", &TimeSeriesType::is_reference)
                .def("has_reference", &TimeSeriesType::has_reference)
                .def("__str__", [](const TimeSeriesType &self) {
                    return fmt::format("TimeSeriesType@{:p}[valid={}, modified={}]",
                                       static_cast<const void *>(&self), self.valid(), self.modified());
                })
                .def("__repr__", [](const TimeSeriesType &self) {
                    return fmt::format("TimeSeriesType@{:p}[valid={}, modified={}]",
                                       static_cast<const void *>(&self), self.valid(), self.modified());
                });
    }

    void TimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesOutput, TimeSeriesType>(m, "TimeSeriesOutput")
                .def_prop_ro("parent_output",
                             [](const TimeSeriesOutput &ts) -> nb::object {
                                 if (ts.has_parent_output()) { return nb::cast(ts.parent_output()); }
                                 return nb::none();
                             })
                .def_prop_ro("has_parent_output", &TimeSeriesOutput::has_parent_output)
                .def_prop_rw("value", &TimeSeriesOutput::py_value, &TimeSeriesOutput::py_set_value,
                             nb::arg("value").none())
                .def("can_apply_result", &TimeSeriesOutput::can_apply_result)
                .def("apply_result", &TimeSeriesOutput::apply_result, nb::arg("value").none())
                .def("invalidate", &TimeSeriesOutput::invalidate)
                .def("mark_invalid", &TimeSeriesOutput::mark_invalid)
                .def("mark_modified", static_cast<void (TimeSeriesOutput::*)()>(&TimeSeriesOutput::mark_modified))
                .def("mark_modified",
                     static_cast<void (TimeSeriesOutput::*)(engine_time_t)>(&TimeSeriesOutput::mark_modified))
                .def("subscribe", &TimeSeriesOutput::subscribe)
                .def("unsubscribe", &TimeSeriesOutput::un_subscribe)
                .def("copy_from_output", &TimeSeriesOutput::copy_from_output)
                .def("copy_from_input", &TimeSeriesOutput::copy_from_input)
                .def("__str__", [](const TimeSeriesOutput &self) {
                    return fmt::format("TimeSeriesOutput@{:p}[valid={}, modified={}]",
                                       static_cast<const void *>(&self), self.valid(), self.modified());
                })
                .def("__repr__", [](const TimeSeriesOutput &self) {
                    return fmt::format("TimeSeriesOutput@{:p}[valid={}, modified={}]",
                                       static_cast<const void *>(&self), self.valid(), self.modified());
                });
    }

    void TimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesInput, TimeSeriesType>(m, "TimeSeriesInput")
                .def_prop_ro("parent_input",
                             [](const TimeSeriesInput &ts) -> nb::object {
                                 if (ts.has_parent_input()) { return nb::cast(ts.parent_input()); }
                                 return nb::none();
                             })
                .def_prop_ro("has_parent_input", &TimeSeriesInput::has_parent_input)
                .def_prop_ro("bound", &TimeSeriesInput::bound)
                .def_prop_ro("has_peer", &TimeSeriesInput::has_peer)
                .def_prop_ro("output",
                             [](const TimeSeriesInput &ts) -> nb::object {
                                 if (ts.has_output()) { return nb::cast(ts.output()); }
                                 return nb::none();
                             })
                .def_prop_ro("reference_output",
                             [](const TimeSeriesInput &ts) -> nb::object {
                                 auto ref = ts.reference_output();
                                 if (ref != nullptr) { return nb::cast(ref); }
                                 return nb::none();
                             })
                .def_prop_ro("active", &TimeSeriesInput::active)
                .def("bind_output", &TimeSeriesInput::bind_output, "output"_a)
                .def("un_bind_output", &TimeSeriesInput::un_bind_output, "unbind_refs"_a = false)
                .def("make_active", &TimeSeriesInput::make_active)
                .def("make_passive", &TimeSeriesInput::make_passive)
                .def("__str__", [](const TimeSeriesInput &self) {
                    return fmt::format("TimeSeriesInput@{:p}[bound={}, valid={}, active={}]",
                                       static_cast<const void *>(&self), self.bound(), self.valid(), self.active());
                })
                .def("__repr__", [](const TimeSeriesInput &self) {
                    return fmt::format("TimeSeriesInput@{:p}[bound={}, valid={}, active={}]",
                                       static_cast<const void *>(&self), self.bound(), self.valid(), self.active());
                });
    }
} // namespace hgraph
