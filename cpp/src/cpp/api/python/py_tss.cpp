#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tss.h>

namespace hgraph
{

    // ============================================================================
    // PyTimeSeriesSetOutput implementation
    // ============================================================================

    PyTimeSeriesSetOutput::PyTimeSeriesSetOutput(api_ptr impl_)
        : PyTimeSeriesOutput(std::move(impl_)) {}

    TimeSeriesSetOutput* PyTimeSeriesSetOutput::impl() const {
        return static_cast_impl<TimeSeriesSetOutput>();
    }

    bool PyTimeSeriesSetOutput::contains(const nb::object &item) const {
        return impl()->py_contains(item);
    }

    size_t PyTimeSeriesSetOutput::size() const {
        return impl()->size();
    }

    nb::bool_ PyTimeSeriesSetOutput::empty() const {
        return nb::bool_(impl()->empty());
    }

    nb::object PyTimeSeriesSetOutput::value() const {
        return impl()->py_value();
    }

    nb::object PyTimeSeriesSetOutput::values() const {
        return value();
    }

    nb::object PyTimeSeriesSetOutput::added() const {
        return impl()->py_added();
    }

    nb::object PyTimeSeriesSetOutput::removed() const {
        return impl()->py_removed();
    }

    nb::bool_ PyTimeSeriesSetOutput::was_added(const nb::object &item) const {
        return nb::bool_(impl()->py_was_added(item));
    }

    nb::bool_ PyTimeSeriesSetOutput::was_removed(const nb::object &item) const {
        return nb::bool_(impl()->py_was_removed(item));
    }

    void PyTimeSeriesSetOutput::remove(const nb::object &key) const {
        if (key.is_none()) return;
        impl()->py_remove(key);
    }

    void PyTimeSeriesSetOutput::add(const nb::object &key) const {
        if (key.is_none()) return;
        impl()->py_add(key);
    }

    nb::object PyTimeSeriesSetOutput::get_contains_output(const nb::object &item, const nb::object &requester) const {
        return wrap_output(impl()->get_contains_output(item, requester));
    }

    void PyTimeSeriesSetOutput::release_contains_output(const nb::object &item, const nb::object &requester) const {
        impl()->release_contains_output(item, requester);
    }

    nb::object PyTimeSeriesSetOutput::is_empty_output() const {
        return wrap_output(impl()->is_empty_output());
    }

    nb::str PyTimeSeriesSetOutput::py_str() const {
        auto self = impl();
        auto s = fmt::format("TimeSeriesSetOutput@{:p}[size={}, valid={}]",
                             static_cast<const void *>(self), self->size(), self->valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetOutput::py_repr() const {
        return py_str();
    }

    // ============================================================================
    // PyTimeSeriesSetInput implementation
    // ============================================================================

    PyTimeSeriesSetInput::PyTimeSeriesSetInput(api_ptr impl_)
        : PyTimeSeriesInput(std::move(impl_)) {}

    TimeSeriesSetInput* PyTimeSeriesSetInput::impl() const {
        return static_cast_impl<TimeSeriesSetInput>();
    }

    bool PyTimeSeriesSetInput::contains(const nb::object &item) const {
        return impl()->py_contains(item);
    }

    size_t PyTimeSeriesSetInput::size() const {
        return impl()->size();
    }

    nb::bool_ PyTimeSeriesSetInput::empty() const {
        return nb::bool_(impl()->empty());
    }

    nb::object PyTimeSeriesSetInput::value() const {
        return impl()->py_value();
    }

    nb::object PyTimeSeriesSetInput::values() const {
        return value();
    }

    nb::object PyTimeSeriesSetInput::added() const {
        return impl()->py_added();
    }

    nb::object PyTimeSeriesSetInput::removed() const {
        return impl()->py_removed();
    }

    nb::bool_ PyTimeSeriesSetInput::was_added(const nb::object &item) const {
        return nb::bool_(impl()->py_was_added(item));
    }

    nb::bool_ PyTimeSeriesSetInput::was_removed(const nb::object &item) const {
        return nb::bool_(impl()->py_was_removed(item));
    }

    nb::str PyTimeSeriesSetInput::py_str() const {
        auto self = impl();
        auto s = fmt::format("TimeSeriesSetInput@{:p}[size={}, valid={}]",
                             static_cast<const void *>(self), self->size(), self->valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetInput::py_repr() const {
        return py_str();
    }

    // ============================================================================
    // nanobind registration
    // ============================================================================

    void tss_register_with_nanobind(nb::module_ &m) {
        // Register output wrapper
        auto tss_o = nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput");
        tss_o.def("__contains__", &PyTimeSeriesSetOutput::contains)
            .def("__len__", &PyTimeSeriesSetOutput::size)
            .def("empty", &PyTimeSeriesSetOutput::empty)
            .def("values", &PyTimeSeriesSetOutput::values)
            .def("added", &PyTimeSeriesSetOutput::added)
            .def("removed", &PyTimeSeriesSetOutput::removed)
            .def("was_added", &PyTimeSeriesSetOutput::was_added)
            .def("was_removed", &PyTimeSeriesSetOutput::was_removed)
            .def("__str__", &PyTimeSeriesSetOutput::py_str)
            .def("__repr__", &PyTimeSeriesSetOutput::py_repr)
            .def("add", &PyTimeSeriesSetOutput::add)
            .def("remove", &PyTimeSeriesSetOutput::remove, "key"_a)
            .def("is_empty_output", &PyTimeSeriesSetOutput::is_empty_output)
            .def("get_contains_output", &PyTimeSeriesSetOutput::get_contains_output)
            .def("release_contains_output", &PyTimeSeriesSetOutput::release_contains_output);

        // Register input wrapper
        auto tss_i = nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput");
        tss_i.def("__contains__", &PyTimeSeriesSetInput::contains)
            .def("__len__", &PyTimeSeriesSetInput::size)
            .def("empty", &PyTimeSeriesSetInput::empty)
            .def("values", &PyTimeSeriesSetInput::values)
            .def("added", &PyTimeSeriesSetInput::added)
            .def("removed", &PyTimeSeriesSetInput::removed)
            .def("was_added", &PyTimeSeriesSetInput::was_added)
            .def("was_removed", &PyTimeSeriesSetInput::was_removed)
            .def("__str__", &PyTimeSeriesSetInput::py_str)
            .def("__repr__", &PyTimeSeriesSetInput::py_repr);
    }

} // namespace hgraph
