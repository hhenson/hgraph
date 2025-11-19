#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tss.h>

namespace hgraph
{

    PyTimeSeriesSetOutput::PyTimeSeriesSetOutput(TimeSeriesSetOutput *o, control_block_ptr cb)
        : PyTimeSeriesSet<PyTimeSeriesOutput>(o, std::move(cb)) {}

    PyTimeSeriesSetOutput::PyTimeSeriesSetOutput(TimeSeriesSetOutput *o) : PyTimeSeriesSet<PyTimeSeriesOutput>(o) {}

    void PyTimeSeriesSetOutput::remove(const nb::object &key) const { impl()->py_remove(key); }

    void PyTimeSeriesSetOutput::add(const nb::object &key) const { impl()->py_add(key); }

    nb::object PyTimeSeriesSetOutput::get_contains_output(const nb::object &item, const nb::object &requester) const {
        return wrap_output(impl()->get_contains_output(item, requester).get(), control_block());
    }

    void PyTimeSeriesSetOutput::release_contains_output(const nb::object &item, const nb::object &requester) const {
        impl()->release_contains_output(item, requester);
    }

    nb::object PyTimeSeriesSetOutput::is_empty_output() const {
        return wrap_output(impl()->is_empty_output().get(), control_block());
    }

    nb::str PyTimeSeriesSetOutput::py_str() const {
        auto self{impl()};
        auto s{fmt::format("TimeSeriesSetOutput@{:p}[size={}, valid={}]", static_cast<const void *>(self), self->size(),
                           self->valid())};
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetOutput::py_repr() const { return py_str(); }

    TimeSeriesSetOutput *PyTimeSeriesSetOutput::impl() const { return this->template static_cast_impl<TimeSeriesSetOutput>(); }

    PyTimeSeriesSetInput::PyTimeSeriesSetInput(TimeSeriesSetInput *o, control_block_ptr cb)
        : PyTimeSeriesSet<PyTimeSeriesInput>(o, std::move(cb)) {}

    PyTimeSeriesSetInput::PyTimeSeriesSetInput(TimeSeriesSetInput *o) : PyTimeSeriesSet<PyTimeSeriesInput>(o) {}

    nb::str PyTimeSeriesSetInput::py_str() const {
        auto self{impl()};
        auto s =
            fmt::format("TimeSeriesSetInput@{:p}[size={}, valid={}]", static_cast<const void *>(self), self->size(), self->valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetInput::py_repr() const { return py_str(); }
    TimeSeriesSetInput *PyTimeSeriesSetInput::impl() const {
        return static_cast_impl<TimeSeriesSetInput>();
    }

    template <typename T, typename... U> void _add_base_methods_to_tss(nb::class_<T, U...> &cls) {
        cls.def("__contains__", &T::contains)
            .def("__len__", &T::size)
            .def("empty", &T::empty)
            .def("values", &T::values)
            .def("added", &T::added)
            .def("removed", &T::removed)
            .def("was_added", &T::was_added)
            .def("was_removed", &T::was_removed)
            .def("__str__", &T::py_str)
            .def("__repr__", &T::py_repr);
    }

    void tss_register_with_nanobind(nb::module_ &m) {
        auto tss_i = nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput");
        _add_base_methods_to_tss(tss_i);

        auto tss_o = nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput");
        _add_base_methods_to_tss(tss_o);
        tss_o.def("add", &PyTimeSeriesSetOutput::add)
            .def("remove", &PyTimeSeriesSetOutput::remove, "key"_a)
            .def("is_empty_output", &PyTimeSeriesSetOutput::is_empty_output)
            .def("get_contains_output", &PyTimeSeriesSetOutput::get_contains_output)
            .def("release_contains_output", &PyTimeSeriesSetOutput::release_contains_output);
    }

}  // namespace hgraph
