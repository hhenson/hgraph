#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_set_view.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/view_data.h>

namespace hgraph
{

    // Helper to convert Python element to value::Value using TSMeta
    // meta->value_type is the Set TypeMeta; element_type is the per-element TypeMeta
    static value::Value<> elem_from_python(const nb::object &elem, const TSMeta* meta) {
        const auto *elem_schema = meta->value_type->element_type;
        value::Value<> elem_val(elem_schema);
        elem_schema->ops->from_python(elem_val.data(), elem, elem_schema);
        return elem_val;
    }

    // Helper to convert value::View element to Python
    static nb::object elem_to_python(const value::View& elem, const TSMeta* meta) {
        const auto *elem_schema = meta->value_type->element_type;
        return elem_schema->ops->to_python(elem.data(), elem_schema);
    }

    // ===== PyTimeSeriesSetOutput Implementation =====

    bool PyTimeSeriesSetOutput::contains(const nb::object &item) const {
        auto set_view = view().as_set();
        auto elem_val = elem_from_python(item, set_view.meta());
        return set_view.contains(elem_val.const_view());
    }

    size_t PyTimeSeriesSetOutput::size() const {
        return view().as_set().size();
    }

    nb::bool_ PyTimeSeriesSetOutput::empty() const {
        return nb::bool_(view().as_set().size() == 0);
    }

    nb::object PyTimeSeriesSetOutput::value() const {
        auto set_view = view().as_set();
        nb::set result;
        for (auto elem : set_view.values()) {
            result.add(elem_to_python(elem, set_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesSetOutput::values() const {
        auto set_view = view().as_set();
        nb::set result;
        for (auto elem : set_view.values()) {
            result.add(elem_to_python(elem, set_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesSetOutput::added() const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) return nb::frozenset(nb::set());
        auto added = delta.attr("added");
        if (nb::isinstance<nb::frozenset>(added)) return added;
        return nb::frozenset(nb::cast<nb::set>(added));
    }

    nb::object PyTimeSeriesSetOutput::removed() const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) return nb::frozenset(nb::set());
        auto removed = delta.attr("removed");
        if (nb::isinstance<nb::frozenset>(removed)) return removed;
        return nb::frozenset(nb::cast<nb::set>(removed));
    }

    nb::bool_ PyTimeSeriesSetOutput::was_added(const nb::object &item) const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) return nb::bool_(false);
        nb::object added = nb::borrow(delta.attr("added"));
        return nb::bool_(PySequence_Contains(added.ptr(), item.ptr()) == 1);
    }

    nb::bool_ PyTimeSeriesSetOutput::was_removed(const nb::object &item) const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) return nb::bool_(false);
        nb::object removed = nb::borrow(delta.attr("removed"));
        return nb::bool_(PySequence_Contains(removed.ptr(), item.ptr()) == 1);
    }

    void PyTimeSeriesSetOutput::add(const nb::object &key) const {
        auto set_view = output_view().ts_view().as_set();
        auto elem_val = elem_from_python(key, set_view.meta());
        set_view.add(elem_val.const_view());
    }

    void PyTimeSeriesSetOutput::remove(const nb::object &key) const {
        auto set_view = output_view().ts_view().as_set();
        auto elem_val = elem_from_python(key, set_view.meta());
        set_view.remove(elem_val.const_view());
    }

    nb::object PyTimeSeriesSetOutput::get_contains_output(const nb::object &item, const nb::object &requester) const {
        throw std::runtime_error("not implemented: PyTimeSeriesSetOutput::get_contains_output");
    }

    void PyTimeSeriesSetOutput::release_contains_output(const nb::object &item, const nb::object &requester) const {
        throw std::runtime_error("not implemented: PyTimeSeriesSetOutput::release_contains_output");
    }

    nb::object PyTimeSeriesSetOutput::is_empty_output() const {
        throw std::runtime_error("not implemented: PyTimeSeriesSetOutput::is_empty_output");
    }

    nb::str PyTimeSeriesSetOutput::py_str() const {
        auto set_view = view().as_set();
        auto s = fmt::format("TimeSeriesSetOutput@{:p}[size={}, valid={}]",
                             static_cast<const void *>(&set_view), set_view.size(), set_view.valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetOutput::py_repr() const { return py_str(); }

    // ===== PyTimeSeriesSetInput Implementation =====

    // Helper: get resolved ViewData that follows the link target to the bound output.
    // ts_ops functions handle link delegation, but TSSView reads ViewData directly.
    // This helper uses ts_ops::value() to get the resolved value::View, then
    // uses its data pointer to identify the correct SetStorage.
    static ViewData resolve_input_view_data(const PyTimeSeriesSetInput& input) {
        auto ts = input.input_view().ts_view();
        const auto& vd = ts.view_data();

        // Use ts_ops::value() which follows links to get the resolved data
        auto resolved_value = vd.ops->value(vd);
        auto resolved_delta = vd.ops->delta_value(vd);

        // If value data differs, the link was followed
        if (resolved_value.data() != vd.value_data || !vd.value_data) {
            ViewData resolved;
            resolved.value_data = const_cast<void*>(resolved_value.data());
            resolved.delta_data = const_cast<void*>(resolved_delta.data());
            // For time_data, use the link target directly since ts_ops::value doesn't expose it
            if (vd.uses_link_target && vd.link_data) {
                auto* lt = static_cast<const LinkTarget*>(vd.link_data);
                if (lt && lt->valid()) {
                    resolved.time_data = lt->time_data;
                    resolved.observer_data = lt->observer_data;
                    resolved.link_data = lt->link_data;
                    resolved.meta = lt->meta ? lt->meta : vd.meta;
                    resolved.ops = lt->ops ? lt->ops : vd.ops;
                } else {
                    resolved.time_data = vd.time_data;
                    resolved.observer_data = vd.observer_data;
                    resolved.link_data = nullptr;
                    resolved.meta = vd.meta;
                    resolved.ops = vd.ops;
                }
            } else {
                resolved.time_data = vd.time_data;
                resolved.observer_data = vd.observer_data;
                resolved.link_data = nullptr;
                resolved.meta = vd.meta;
                resolved.ops = vd.ops;
            }
            resolved.path = vd.path;
            resolved.uses_link_target = false;
            resolved.sampled = vd.sampled;
            return resolved;
        }
        return vd;
    }

    bool PyTimeSeriesSetInput::contains(const nb::object &item) const {
        auto resolved_vd = resolve_input_view_data(*this);
        auto set_view = TSSView(resolved_vd, input_view().current_time());
        auto elem_val = elem_from_python(item, set_view.meta());
        return set_view.contains(elem_val.const_view());
    }

    size_t PyTimeSeriesSetInput::size() const {
        auto resolved_vd = resolve_input_view_data(*this);
        return TSSView(resolved_vd, input_view().current_time()).size();
    }

    nb::bool_ PyTimeSeriesSetInput::empty() const {
        auto resolved_vd = resolve_input_view_data(*this);
        return nb::bool_(TSSView(resolved_vd, input_view().current_time()).size() == 0);
    }

    nb::object PyTimeSeriesSetInput::value() const {
        // Use ts_ops dispatch which follows links
        auto ts = input_view().ts_view();
        auto py_val = ts.to_python();
        if (py_val.is_none()) return nb::set();
        // SetOps::to_python returns frozenset; TSS.value should be mutable set
        if (nb::isinstance<nb::frozenset>(py_val)) {
            return nb::set(py_val);
        }
        return py_val;
    }

    nb::object PyTimeSeriesSetInput::values() const {
        auto ts = input_view().ts_view();
        auto py_val = ts.to_python();
        if (py_val.is_none()) return nb::frozenset(nb::set());
        // Return as frozenset
        if (nb::isinstance<nb::set>(py_val)) {
            return nb::frozenset(nb::cast<nb::set>(py_val));
        }
        return py_val;
    }

    // Helper: get the PythonSetDelta from the resolved output via delta_to_python
    static nb::object get_input_delta(const PyTimeSeriesSetInput& input) {
        auto ts = input.input_view().ts_view();
        return ts.delta_to_python();
    }

    nb::object PyTimeSeriesSetInput::added() const {
        auto delta = get_input_delta(*this);
        if (delta.is_none()) return nb::frozenset(nb::set());
        auto added = delta.attr("added");
        // PythonSetDelta.added returns frozenset or set depending on emptiness
        if (nb::isinstance<nb::frozenset>(added)) return added;
        return nb::frozenset(nb::cast<nb::set>(added));
    }

    nb::object PyTimeSeriesSetInput::removed() const {
        auto delta = get_input_delta(*this);
        if (delta.is_none()) return nb::frozenset(nb::set());
        auto removed = delta.attr("removed");
        if (nb::isinstance<nb::frozenset>(removed)) return removed;
        return nb::frozenset(nb::cast<nb::set>(removed));
    }

    nb::bool_ PyTimeSeriesSetInput::was_added(const nb::object &item) const {
        auto delta = get_input_delta(*this);
        if (delta.is_none()) return nb::bool_(false);
        nb::object added = nb::borrow(delta.attr("added"));
        // Use Python 'in' operator
        return nb::bool_(PySequence_Contains(added.ptr(), item.ptr()) == 1);
    }

    nb::bool_ PyTimeSeriesSetInput::was_removed(const nb::object &item) const {
        auto delta = get_input_delta(*this);
        if (delta.is_none()) return nb::bool_(false);
        nb::object removed = nb::borrow(delta.attr("removed"));
        return nb::bool_(PySequence_Contains(removed.ptr(), item.ptr()) == 1);
    }

    nb::str PyTimeSeriesSetInput::py_str() const {
        auto set_view = input_view().ts_view().as_set();
        auto s = fmt::format("TimeSeriesSetInput@{:p}[size={}, valid={}]",
                             static_cast<const void *>(&set_view), set_view.size(), set_view.valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetInput::py_repr() const { return py_str(); }

    void tss_register_with_nanobind(nb::module_ &m) {
        // Register non-templated wrapper classes
        auto tss_input = nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput");
        tss_input.def_prop_ro("value", &PyTimeSeriesSetInput::value)
            .def("__contains__", &PyTimeSeriesSetInput::contains)
            .def("__len__", &PyTimeSeriesSetInput::size)
            .def("empty", &PyTimeSeriesSetInput::empty)
            .def("values", &PyTimeSeriesSetInput::values)
            .def("added", &PyTimeSeriesSetInput::added)
            .def("removed", &PyTimeSeriesSetInput::removed)
            .def("was_added", &PyTimeSeriesSetInput::was_added)
            .def("was_removed", &PyTimeSeriesSetInput::was_removed)
            .def("__str__", &PyTimeSeriesSetInput::py_str)
            .def("__repr__", &PyTimeSeriesSetInput::py_repr);

        auto tss_output = nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput");
        tss_output.def_prop_rw("value", &PyTimeSeriesSetOutput::value, &PyTimeSeriesOutput::set_value, nb::arg("value").none())
            .def("__contains__", &PyTimeSeriesSetOutput::contains)
            .def("__len__", &PyTimeSeriesSetOutput::size)
            .def("empty", &PyTimeSeriesSetOutput::empty)
            .def("values", &PyTimeSeriesSetOutput::values)
            .def("added", &PyTimeSeriesSetOutput::added)
            .def("removed", &PyTimeSeriesSetOutput::removed)
            .def("was_added", &PyTimeSeriesSetOutput::was_added)
            .def("was_removed", &PyTimeSeriesSetOutput::was_removed)
            .def("add", &PyTimeSeriesSetOutput::add)
            .def("remove", &PyTimeSeriesSetOutput::remove, "key"_a)
            .def("is_empty_output", &PyTimeSeriesSetOutput::is_empty_output)
            .def("get_contains_output", &PyTimeSeriesSetOutput::get_contains_output)
            .def("release_contains_output", &PyTimeSeriesSetOutput::release_contains_output)
            .def("__str__", &PyTimeSeriesSetOutput::py_str)
            .def("__repr__", &PyTimeSeriesSetOutput::py_repr);
    }

} // namespace hgraph
