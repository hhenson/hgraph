#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph
{

    // ========== PyTimeSeriesSetOutput ==========

    PyTimeSeriesSetOutput::PyTimeSeriesSetOutput(TSMutableView view)
        : PyTimeSeriesOutput(view) {}

    bool PyTimeSeriesSetOutput::contains(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesSetOutput::contains not yet implemented for view-based wrappers");
    }

    size_t PyTimeSeriesSetOutput::size() const {
        TSSView set = _view.as_set();
        return set.size();
    }

    nb::bool_ PyTimeSeriesSetOutput::empty() const {
        return nb::bool_(size() == 0);
    }

    nb::object PyTimeSeriesSetOutput::values() const {
        throw std::runtime_error("PyTimeSeriesSetOutput::values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesSetOutput::added() const {
        throw std::runtime_error("PyTimeSeriesSetOutput::added not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesSetOutput::removed() const {
        throw std::runtime_error("PyTimeSeriesSetOutput::removed not yet implemented for view-based wrappers");
    }

    nb::bool_ PyTimeSeriesSetOutput::was_added(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesSetOutput::was_added not yet implemented for view-based wrappers");
    }

    nb::bool_ PyTimeSeriesSetOutput::was_removed(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesSetOutput::was_removed not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesSetOutput::add(const nb::object &key) const {
        throw std::runtime_error("PyTimeSeriesSetOutput::add not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesSetOutput::remove(const nb::object &key) const {
        throw std::runtime_error("PyTimeSeriesSetOutput::remove not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesSetOutput::get_contains_output(const nb::object &item, const nb::object &requester) const {
        throw std::runtime_error("PyTimeSeriesSetOutput::get_contains_output not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesSetOutput::release_contains_output(const nb::object &item, const nb::object &requester) const {
        throw std::runtime_error("PyTimeSeriesSetOutput::release_contains_output not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesSetOutput::is_empty_output() const {
        throw std::runtime_error("PyTimeSeriesSetOutput::is_empty_output not yet implemented for view-based wrappers");
    }

    // value() and delta_value() are inherited from base - view layer handles TSS specifics

    nb::str PyTimeSeriesSetOutput::py_str() const {
        auto s = fmt::format("TimeSeriesSetOutput[size={}, valid={}]", size(), _view.ts_valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetOutput::py_repr() const { return py_str(); }

    // ========== PyTimeSeriesSetInput ==========

    PyTimeSeriesSetInput::PyTimeSeriesSetInput(TSView view)
        : PyTimeSeriesInput(view) {}

    bool PyTimeSeriesSetInput::contains(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesSetInput::contains not yet implemented for view-based wrappers");
    }

    size_t PyTimeSeriesSetInput::size() const {
        TSSView set = _view.as_set();
        return set.size();
    }

    nb::bool_ PyTimeSeriesSetInput::empty() const {
        return nb::bool_(size() == 0);
    }

    nb::object PyTimeSeriesSetInput::values() const {
        throw std::runtime_error("PyTimeSeriesSetInput::values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesSetInput::added() const {
        throw std::runtime_error("PyTimeSeriesSetInput::added not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesSetInput::removed() const {
        throw std::runtime_error("PyTimeSeriesSetInput::removed not yet implemented for view-based wrappers");
    }

    nb::bool_ PyTimeSeriesSetInput::was_added(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesSetInput::was_added not yet implemented for view-based wrappers");
    }

    nb::bool_ PyTimeSeriesSetInput::was_removed(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesSetInput::was_removed not yet implemented for view-based wrappers");
    }

    // value() and delta_value() are inherited from base - view layer handles TSS specifics

    nb::str PyTimeSeriesSetInput::py_str() const {
        auto s = fmt::format("TimeSeriesSetInput[size={}, valid={}]", size(), _view.ts_valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetInput::py_repr() const { return py_str(); }

    // ========== Nanobind Registration ==========

    void tss_register_with_nanobind(nb::module_ &m) {
        // Register non-templated wrapper classes
        auto tss_input = nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput");
        // No need to re-register value property - base class handles it via TSView
        tss_input
            .def("__contains__", &PyTimeSeriesSetInput::contains)
            .def("__len__", &PyTimeSeriesSetInput::size)
            .def("empty", &PyTimeSeriesSetInput::empty)
            .def("values", &PyTimeSeriesSetInput::values)
            .def("added", &PyTimeSeriesSetInput::added)
            .def("removed", &PyTimeSeriesSetInput::removed)
            .def("was_added", &PyTimeSeriesSetInput::was_added)
            .def("was_removed", &PyTimeSeriesSetInput::was_removed)
            // value and delta_value are inherited from base class (uses view layer dispatch)
            .def("__str__", &PyTimeSeriesSetInput::py_str)
            .def("__repr__", &PyTimeSeriesSetInput::py_repr);

        auto tss_output = nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput");
        // No need to re-register value property - base class handles it via TSView
        tss_output
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
            // value and delta_value are inherited from base class (uses view layer dispatch)
            .def("__str__", &PyTimeSeriesSetOutput::py_str)
            .def("__repr__", &PyTimeSeriesSetOutput::py_repr);
    }

} // namespace hgraph
