#include "hgraph/api/python/wrapper_factory.h"
#include "hgraph/types/time_series_type.h"

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>

namespace hgraph
{
    nb::object PyTimeSeriesType::owning_node() const {
        auto n = _impl->owning_node();
        return n ? wrap_node(n->shared_from_this()) : nb::none();
    }

    nb::object PyTimeSeriesType::owning_graph() const {
        auto g = _impl->owning_graph();
        return g ? wrap_graph(g->shared_from_this()) : nb::none();
    }

    nb::bool_ PyTimeSeriesType::has_parent_or_node() const { return nb::bool_(_impl->has_parent_or_node()); }

    nb::bool_ PyTimeSeriesType::has_owning_node() const { return nb::bool_(_impl->has_owning_node()); }

    nb::object PyTimeSeriesType::value() const {
        // TODO: I would like to extract the python logic into this function, to use a visitor function.
        return _impl->py_value();
    }

    nb::object PyTimeSeriesType::delta_value() const { return _impl->py_delta_value(); }

    engine_time_t PyTimeSeriesType::last_modified_time() const { return _impl->last_modified_time(); }

    nb::bool_ PyTimeSeriesType::valid() const { return nb::bool_(_impl->valid()); }

    nb::bool_ PyTimeSeriesType::all_valid() const { return nb::bool_(_impl->all_valid()); }

    nb::bool_ PyTimeSeriesType::is_reference() const { return nb::bool_(_impl->is_reference()); }

    nb::bool_ PyTimeSeriesType::modified() const { return nb::bool_(_impl->modified()); }

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
            .def("is_reference", &PyTimeSeriesType::is_reference);
        //.def("has_reference", &PyTimeSeriesType::has_reference)
    }

    PyTimeSeriesType::PyTimeSeriesType(api_ptr impl) : _impl{std::move(impl)} {}

    control_block_ptr PyTimeSeriesType::control_block() const { return _impl.control_block(); }

    nb::object PyTimeSeriesOutput::parent_output() const { return impl()->parent_output() ? wrap_output(impl()->parent_output()) : nb::none(); }

    nb::bool_ PyTimeSeriesOutput::has_parent_output() const { return nb::bool_(impl()->has_parent_output()); }

    void PyTimeSeriesOutput::apply_result(nb::object value) { impl()->apply_result(std::move(value)); }

    void PyTimeSeriesOutput::set_value(nb::object value) { impl()->py_set_value(std::move(value)); }

    void PyTimeSeriesOutput::copy_from_output(const PyTimeSeriesOutput &output) {
        impl()->copy_from_output(*unwrap_output(output));
    }

    void PyTimeSeriesOutput::copy_from_input(const PyTimeSeriesInput &input) { impl()->copy_from_input(*unwrap_input(input)); }

    void PyTimeSeriesOutput::clear() { impl()->clear(); }

    void PyTimeSeriesOutput::invalidate() { impl()->invalidate(); }

    bool PyTimeSeriesOutput::can_apply_result(nb::object value) { return impl()->can_apply_result(std::move(value)); }


    void PyTimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesOutput, PyTimeSeriesType>(m, "TimeSeriesOutput")
            .def_prop_ro("parent_output", &PyTimeSeriesOutput::parent_output)
            .def_prop_ro("has_parent_output", &PyTimeSeriesOutput::has_parent_output)
            .def_prop_rw("value", &PyTimeSeriesOutput::value, &PyTimeSeriesOutput::set_value, nb::arg("value").none())
            .def("can_apply_result", &PyTimeSeriesOutput::can_apply_result)
            .def("apply_result", &PyTimeSeriesOutput::apply_result, nb::arg("value").none())
            .def("clear", &PyTimeSeriesOutput::clear)
            .def("invalidate", &PyTimeSeriesOutput::invalidate)
            // .def("mark_invalid", &PyTimeSeriesOutput::mark_invalid)
            // .def("mark_modified", static_cast<void (PyTimeSeriesOutput::*)()>(&PyTimeSeriesOutput::mark_modified))
            // .def("mark_modified", static_cast<void (PyTimeSeriesOutput::*)(engine_time_t)>(&PyTimeSeriesOutput::mark_modified))
            // .def("subscribe", &PyTimeSeriesOutput::subscribe)
            // .def("unsubscribe", &PyTimeSeriesOutput::un_subscribe)
            .def("copy_from_output", &PyTimeSeriesOutput::copy_from_output)
            .def("copy_from_input", &PyTimeSeriesOutput::copy_from_input);
    }

    TimeSeriesOutput *PyTimeSeriesOutput::impl() const { return static_cast_impl<TimeSeriesOutput>(); }

    nb::object PyTimeSeriesInput::parent_input() const { return impl()->parent_input() ? wrap_input(impl()->parent_input()->shared_from_this()) : nb::none(); }

    nb::bool_ PyTimeSeriesInput::has_parent_input() const { return nb::bool_(impl()->has_parent_input()); }

    nb::bool_ PyTimeSeriesInput::active() const { return nb::bool_(impl()->active()); }

    void PyTimeSeriesInput::make_active() { impl()->make_active(); }

    void PyTimeSeriesInput::make_passive() { impl()->make_passive(); }

    nb::bool_ PyTimeSeriesInput::bound() const { return nb::bool_(impl()->bound()); }

    nb::bool_ PyTimeSeriesInput::has_peer() const { return nb::bool_(impl()->has_peer()); }

    nb::object PyTimeSeriesInput::output() const { return wrap_output(impl()->output() ? impl()->output()->shared_from_this() : nullptr); }

    nb::bool_ PyTimeSeriesInput::has_output() const { return nb::bool_(impl()->has_output()); }

    nb::bool_ PyTimeSeriesInput::bind_output(nb::object output_) { return nb::bool_(impl()->bind_output(unwrap_output(output_))); }

    void PyTimeSeriesInput::un_bind_output(bool unbind_refs) { return impl()->un_bind_output(unbind_refs); }

    nb::object PyTimeSeriesInput::reference_output() const {
        return wrap_output(impl()->reference_output());
    }

    nb::object PyTimeSeriesInput::get_input(size_t index) const { return wrap_input(impl()->get_input(index)); }

    void PyTimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesInput, PyTimeSeriesType>(m, "TimeSeriesInput")
            .def_prop_ro("parent_input", &PyTimeSeriesInput::parent_input)
            .def_prop_ro("has_parent_input", &PyTimeSeriesInput::has_parent_input)
            .def_prop_ro("bound", &PyTimeSeriesInput::bound)
            .def_prop_ro("has_peer", &PyTimeSeriesInput::has_peer)
            .def_prop_ro("output", &PyTimeSeriesInput::output)
            .def_prop_ro("reference_output", &PyTimeSeriesInput::reference_output)
            .def_prop_ro("active", &PyTimeSeriesInput::active)
            .def("__getitem__", &PyTimeSeriesInput::get_input, "index"_a)
            .def("bind_output", &PyTimeSeriesInput::bind_output, "output"_a)
            .def("un_bind_output", &PyTimeSeriesInput::un_bind_output, "unbind_refs"_a = false)
            .def("make_active", &PyTimeSeriesInput::make_active)
            .def("make_passive", &PyTimeSeriesInput::make_passive);
    }

    TimeSeriesInput *PyTimeSeriesInput::impl() const { return static_cast_impl<TimeSeriesInput>(); }
}  // namespace hgraph
