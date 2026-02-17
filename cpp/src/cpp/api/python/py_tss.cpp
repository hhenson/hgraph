#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/py_ts_runtime_internal.h>
#include <hgraph/api/python/wrapper_factory.h>

#include <fmt/format.h>

namespace hgraph
{
namespace
{
    value::Value elem_from_python(const nb::object &elem, const TSMeta *meta) {
        const value::TypeMeta* elem_schema = nullptr;
        if (meta != nullptr) {
            if (meta->kind == TSKind::TSS && meta->value_type != nullptr) {
                elem_schema = meta->value_type->element_type;
            } else if (meta->kind == TSKind::TSD) {
                // TSD key-set projections are exposed through set wrappers.
                elem_schema = meta->key_type();
            }
        }
        if (elem_schema == nullptr) {
            return {};
        }

        value::Value elem_val(elem_schema);
        elem_val.emplace();
        elem_schema->ops().from_python(elem_val.data(), elem, elem_schema);
        return elem_val;
    }

    nb::set current_set_values(const TSView &view) {
        nb::set result;
        value::View v = view.value();
        if (!v.valid()) {
            return result;
        }
        if (v.is_set()) {
            for (value::View elem : v.as_set()) {
                result.add(elem.to_python());
            }
            return result;
        }
        if (v.is_map()) {
            for (value::View key : v.as_map().keys()) {
                result.add(key.to_python());
            }
        }
        return result;
    }
}  // namespace

    // ===== PyTimeSeriesSetOutput Implementation =====

    bool PyTimeSeriesSetOutput::contains(const nb::object &item) const {
        value::View v = view().value();
        if (!v.valid()) {
            return false;
        }
        auto elem_val = elem_from_python(item, view().ts_meta());
        if (elem_val.schema() == nullptr) {
            return false;
        }
        if (v.is_set()) {
            return v.as_set().contains(elem_val.view());
        }
        if (v.is_map()) {
            return v.as_map().contains(elem_val.view());
        }
        return false;
    }

    size_t PyTimeSeriesSetOutput::size() const {
        value::View v = view().value();
        if (!v.valid()) {
            return 0;
        }
        if (v.is_set()) {
            return v.as_set().size();
        }
        if (v.is_map()) {
            return v.as_map().size();
        }
        return 0;
    }

    nb::bool_ PyTimeSeriesSetOutput::empty() const {
        return nb::bool_(size() == 0);
    }

    nb::object PyTimeSeriesSetOutput::value() const {
        return current_set_values(view());
    }

    nb::object PyTimeSeriesSetOutput::values() const {
        return current_set_values(view());
    }

    nb::object PyTimeSeriesSetOutput::added() const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) {
            return nb::frozenset(nb::set());
        }
        auto added = delta.attr("added");
        return nb::isinstance<nb::frozenset>(added) ? added : nb::frozenset(nb::cast<nb::set>(added));
    }

    nb::object PyTimeSeriesSetOutput::removed() const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) {
            return nb::frozenset(nb::set());
        }
        auto removed = delta.attr("removed");
        return nb::isinstance<nb::frozenset>(removed) ? removed : nb::frozenset(nb::cast<nb::set>(removed));
    }

    nb::bool_ PyTimeSeriesSetOutput::was_added(const nb::object &item) const {
        auto added_ = added();
        return nb::bool_(PySequence_Contains(added_.ptr(), item.ptr()) == 1);
    }

    nb::bool_ PyTimeSeriesSetOutput::was_removed(const nb::object &item) const {
        auto removed_ = removed();
        return nb::bool_(PySequence_Contains(removed_.ptr(), item.ptr()) == 1);
    }

    void PyTimeSeriesSetOutput::add(const nb::object &key) const {
        auto elem_val = elem_from_python(key, output_view().ts_meta());
        if (elem_val.schema() == nullptr) {
            return;
        }
        output_view().as_set().add(elem_val.view());
    }

    void PyTimeSeriesSetOutput::remove(const nb::object &key) const {
        auto elem_val = elem_from_python(key, output_view().ts_meta());
        if (elem_val.schema() == nullptr) {
            return;
        }
        output_view().as_set().remove(elem_val.view());
    }

    nb::object PyTimeSeriesSetOutput::get_contains_output(const nb::object &item, const nb::object &requester) const {
        TSOutputView base = output_view();
        auto out = runtime_tss_get_contains_output(base, item, requester);
        return out ? wrap_output_view(std::move(out)) : nb::none();
    }

    void PyTimeSeriesSetOutput::release_contains_output(const nb::object &item, const nb::object &requester) const {
        TSOutputView base = output_view();
        runtime_tss_release_contains_output(base, item, requester);
    }

    nb::object PyTimeSeriesSetOutput::is_empty_output() const {
        TSOutputView base = output_view();
        auto out = runtime_tss_get_is_empty_output(base);
        return out ? wrap_output_view(std::move(out)) : nb::none();
    }

    nb::str PyTimeSeriesSetOutput::py_str() const {
        auto s = fmt::format("TimeSeriesSetOutput@{:p}[size={}, valid={}]",
                             static_cast<const void *>(&output_view()), size(), output_view().valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetOutput::py_repr() const { return py_str(); }

    // ===== PyTimeSeriesSetInput Implementation =====

    bool PyTimeSeriesSetInput::contains(const nb::object &item) const {
        value::View v = view().value();
        if (!v.valid()) {
            return false;
        }
        auto elem_val = elem_from_python(item, view().ts_meta());
        if (elem_val.schema() == nullptr) {
            return false;
        }
        if (v.is_set()) {
            return v.as_set().contains(elem_val.view());
        }
        if (v.is_map()) {
            return v.as_map().contains(elem_val.view());
        }
        return false;
    }

    size_t PyTimeSeriesSetInput::size() const {
        value::View v = view().value();
        if (!v.valid()) {
            return 0;
        }
        if (v.is_set()) {
            return v.as_set().size();
        }
        if (v.is_map()) {
            return v.as_map().size();
        }
        return 0;
    }

    nb::bool_ PyTimeSeriesSetInput::empty() const {
        return nb::bool_(size() == 0);
    }

    nb::object PyTimeSeriesSetInput::value() const {
        return current_set_values(view());
    }

    nb::object PyTimeSeriesSetInput::values() const {
        // Input values are exposed as frozenset in Python semantics.
        return nb::frozenset(current_set_values(view()));
    }

    nb::object PyTimeSeriesSetInput::added() const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) {
            return nb::frozenset(nb::set());
        }
        auto added = delta.attr("added");
        return nb::isinstance<nb::frozenset>(added) ? added : nb::frozenset(nb::cast<nb::set>(added));
    }

    nb::object PyTimeSeriesSetInput::removed() const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) {
            return nb::frozenset(nb::set());
        }
        auto removed = delta.attr("removed");
        return nb::isinstance<nb::frozenset>(removed) ? removed : nb::frozenset(nb::cast<nb::set>(removed));
    }

    nb::bool_ PyTimeSeriesSetInput::was_added(const nb::object &item) const {
        auto added_ = added();
        return nb::bool_(PySequence_Contains(added_.ptr(), item.ptr()) == 1);
    }

    nb::bool_ PyTimeSeriesSetInput::was_removed(const nb::object &item) const {
        auto removed_ = removed();
        return nb::bool_(PySequence_Contains(removed_.ptr(), item.ptr()) == 1);
    }

    nb::str PyTimeSeriesSetInput::py_str() const {
        auto s = fmt::format("TimeSeriesSetInput@{:p}[size={}, valid={}]",
                             static_cast<const void *>(&input_view()), size(), input_view().valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetInput::py_repr() const { return py_str(); }

    void tss_register_with_nanobind(nb::module_ &m) {
        // Register non-templated wrapper classes
        auto tss_input = nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput");
        tss_input.def("__contains__", &PyTimeSeriesSetInput::contains)
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
        tss_output.def("__contains__", &PyTimeSeriesSetOutput::contains)
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
