#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_set_view.h>

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
        nb::list result;
        for (auto elem : set_view.values()) {
            result.append(elem_to_python(elem, set_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesSetOutput::values() const {
        return value();
    }

    nb::object PyTimeSeriesSetOutput::added() const {
        auto set_view = view().as_set();
        nb::list result;
        for (auto elem : set_view.added()) {
            result.append(elem_to_python(elem, set_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesSetOutput::removed() const {
        auto set_view = view().as_set();
        nb::list result;
        for (auto elem : set_view.removed()) {
            result.append(elem_to_python(elem, set_view.meta()));
        }
        return result;
    }

    nb::bool_ PyTimeSeriesSetOutput::was_added(const nb::object &item) const {
        auto set_view = view().as_set();
        auto elem_val = elem_from_python(item, set_view.meta());
        return nb::bool_(set_view.was_added(elem_val.const_view()));
    }

    nb::bool_ PyTimeSeriesSetOutput::was_removed(const nb::object &item) const {
        auto set_view = view().as_set();
        auto elem_val = elem_from_python(item, set_view.meta());
        return nb::bool_(set_view.was_removed(elem_val.const_view()));
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

    bool PyTimeSeriesSetInput::contains(const nb::object &item) const {
        auto set_view = input_view().ts_view().as_set();
        auto elem_val = elem_from_python(item, set_view.meta());
        return set_view.contains(elem_val.const_view());
    }

    size_t PyTimeSeriesSetInput::size() const {
        return input_view().ts_view().as_set().size();
    }

    nb::bool_ PyTimeSeriesSetInput::empty() const {
        return nb::bool_(input_view().ts_view().as_set().size() == 0);
    }

    nb::object PyTimeSeriesSetInput::value() const {
        auto set_view = input_view().ts_view().as_set();
        nb::list result;
        for (auto elem : set_view.values()) {
            result.append(elem_to_python(elem, set_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesSetInput::values() const {
        return value();
    }

    nb::object PyTimeSeriesSetInput::added() const {
        auto set_view = input_view().ts_view().as_set();
        nb::list result;
        for (auto elem : set_view.added()) {
            result.append(elem_to_python(elem, set_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesSetInput::removed() const {
        auto set_view = input_view().ts_view().as_set();
        nb::list result;
        for (auto elem : set_view.removed()) {
            result.append(elem_to_python(elem, set_view.meta()));
        }
        return result;
    }

    nb::bool_ PyTimeSeriesSetInput::was_added(const nb::object &item) const {
        auto set_view = input_view().ts_view().as_set();
        auto elem_val = elem_from_python(item, set_view.meta());
        return nb::bool_(set_view.was_added(elem_val.const_view()));
    }

    nb::bool_ PyTimeSeriesSetInput::was_removed(const nb::object &item) const {
        auto set_view = input_view().ts_view().as_set();
        auto elem_val = elem_from_python(item, set_view.meta());
        return nb::bool_(set_view.was_removed(elem_val.const_view()));
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
