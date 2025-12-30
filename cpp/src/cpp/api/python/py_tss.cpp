#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tss.h>

namespace hgraph
{
    nb::str PyTimeSeriesSetOutput::py_str() const {
        auto self{impl()};
        auto s{fmt::format("TimeSeriesSetOutput@{:p}[size={}, valid={}]", static_cast<const void *>(self), self->size(),
                           self->valid())};
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetOutput::py_repr() const { return py_str(); }

    nb::str PyTimeSeriesSetInput::py_str() const {
        auto self{impl()};
        auto s = fmt::format("TimeSeriesSetInput@{:p}[size={}, valid={}]", static_cast<const void *>(self), self->size(), self->valid());
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
