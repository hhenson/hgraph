#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    void PyTimeSeriesType::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesType>(m, "TimeSeriesType")
            .def_prop_ro("owning_node", &PyTimeSeriesType::owning_node)
            .def_prop_ro("owning_graph", &PyTimeSeriesType::owning_graph)
            .def_prop_ro("value", &PyTimeSeriesType::value)
            .def_prop_ro("delta_value", &PyTimeSeriesType::delta_value)
            .def_prop_ro("modified", &PyTimeSeriesType::modified)
            .def_prop_ro("valid", &PyTimeSeriesType::valid)
            .def_prop_ro("all_valid", &PyTimeSeriesType::all_valid)
            .def_prop_ro("last_modified_time", &PyTimeSeriesType::last_modified_time)
            // .def("re_parent", static_cast<void (PyTimeSeriesType::*)(const Node::ptr &)>(&PyTimeSeriesType::re_parent))
            // .def("re_parent", static_cast<void (PyTimeSeriesType::*)(const ptr &)>(&PyTimeSeriesType::re_parent))
            .def("is_reference", &PyTimeSeriesType::is_reference)
            //.def("has_reference", &PyTimeSeriesType::has_reference)
            .def("__str__", &PyTimeSeriesType::py_str)
            .def("__repr__", &PyTimeSeriesType::py_repr);
    }

    void PyTimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesOutput, PyTimeSeriesType>(m, "TimeSeriesOutput")
            .def_prop_ro("parent_output", &PyTimeSeriesOutput::parent_output)
            .def_prop_ro("has_parent_output", &PyTimeSeriesOutput::has_parent_output)
            .def_prop_rw("value", &PyTimeSeriesOutput::value, &PyTimeSeriesOutput::set_value, nb::arg("value").none())
            .def("can_apply_result", &PyTimeSeriesOutput::can_apply_result)
            .def("apply_result", &PyTimeSeriesOutput::apply_result, nb::arg("value").none())
            .def("invalidate", &PyTimeSeriesOutput::invalidate)
            // .def("mark_invalid", &PyTimeSeriesOutput::mark_invalid)
            // .def("mark_modified", static_cast<void (PyTimeSeriesOutput::*)()>(&PyTimeSeriesOutput::mark_modified))
            // .def("mark_modified", static_cast<void (PyTimeSeriesOutput::*)(engine_time_t)>(&PyTimeSeriesOutput::mark_modified))
            // .def("subscribe", &PyTimeSeriesOutput::subscribe)
            // .def("unsubscribe", &PyTimeSeriesOutput::un_subscribe)
            .def("copy_from_output", &PyTimeSeriesOutput::copy_from_output)
            .def("copy_from_input", &PyTimeSeriesOutput::copy_from_input);
    }

    void PyTimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesInput, PyTimeSeriesType>(m, "TimeSeriesInput")
            .def_prop_ro("parent_input", &PyTimeSeriesInput::parent_input)
            .def_prop_ro("has_parent_input", &PyTimeSeriesInput::has_parent_input)
            .def_prop_ro("bound", &PyTimeSeriesInput::bound)
            .def_prop_ro("has_peer", &PyTimeSeriesInput::has_peer)
            .def_prop_ro("output", &PyTimeSeriesInput::output)
            .def_prop_ro("reference_output", &PyTimeSeriesInput::reference_output)
            .def_prop_ro("active", &PyTimeSeriesInput::active)
            .def("bind_output", &PyTimeSeriesInput::bind_output, "output"_a)
            .def("un_bind_output", &PyTimeSeriesInput::un_bind_output, "unbind_refs"_a = false)
            .def("make_active", &PyTimeSeriesInput::make_active)
            .def("make_passive", &PyTimeSeriesInput::make_passive);
    }
}  // namespace hgraph
