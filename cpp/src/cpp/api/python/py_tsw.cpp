#include <hgraph/api/python/py_tsw.h>
#include <hgraph/types/value/window_type.h>
#include <hgraph/types/time_series/ts_type_meta.h>

namespace hgraph
{
    // PyTimeSeriesWindowOutput implementations
    nb::object PyTimeSeriesWindowOutput::value_times() const {
        // TODO: Implement via view
        return nb::none();
    }

    engine_time_t PyTimeSeriesWindowOutput::first_modified_time() const {
        // TODO: Implement via view
        return MIN_DT;
    }

    nb::object PyTimeSeriesWindowOutput::window_size() const {
        // TODO: Implement via view
        return nb::none();
    }

    nb::object PyTimeSeriesWindowOutput::min_size() const {
        // TODO: Implement via view
        return nb::none();
    }

    nb::bool_ PyTimeSeriesWindowOutput::has_removed_value() const {
        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Window) {
            return nb::bool_(false);
        }
        auto* storage = static_cast<const value::WindowStorage*>(v.value_view().data());
        return nb::bool_(storage && storage->has_removed_value());
    }

    nb::object PyTimeSeriesWindowOutput::removed_value() const {
        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Window) {
            return nb::none();
        }
        auto* storage = static_cast<const value::WindowStorage*>(v.value_view().data());
        if (!storage || !storage->has_removed_value()) {
            return nb::none();
        }
        // Convert removed value to Python
        auto* elem_type = storage->element_type();
        if (!elem_type || !elem_type->ops || !elem_type->ops->to_python) {
            return nb::none();
        }
        auto* result = static_cast<PyObject*>(elem_type->ops->to_python(storage->removed_value(), elem_type));
        return nb::steal(nb::handle(result));
    }

    nb::int_ PyTimeSeriesWindowOutput::len() const {
        return nb::int_(view().window_size());
    }

    nb::str PyTimeSeriesWindowOutput::py_str() const {
        return nb::str("TSW[...]");
    }

    nb::str PyTimeSeriesWindowOutput::py_repr() const {
        return py_str();
    }

    // PyTimeSeriesWindowInput implementations
    nb::object PyTimeSeriesWindowInput::value_times() const {
        // TODO: Implement via view
        return nb::none();
    }

    engine_time_t PyTimeSeriesWindowInput::first_modified_time() const {
        // TODO: Implement via view
        return MIN_DT;
    }

    nb::bool_ PyTimeSeriesWindowInput::has_removed_value() const {
        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Window) {
            return nb::bool_(false);
        }
        auto* storage = static_cast<const value::WindowStorage*>(v.value_view().data());
        return nb::bool_(storage && storage->has_removed_value());
    }

    nb::object PyTimeSeriesWindowInput::removed_value() const {
        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Window) {
            return nb::none();
        }
        auto* storage = static_cast<const value::WindowStorage*>(v.value_view().data());
        if (!storage || !storage->has_removed_value()) {
            return nb::none();
        }
        // Convert removed value to Python
        auto* elem_type = storage->element_type();
        if (!elem_type || !elem_type->ops || !elem_type->ops->to_python) {
            return nb::none();
        }
        auto* result = static_cast<PyObject*>(elem_type->ops->to_python(storage->removed_value(), elem_type));
        return nb::steal(nb::handle(result));
    }

    nb::bool_ PyTimeSeriesWindowInput::all_valid() const {
        auto v = view();
        if (!v.valid()) {
            return nb::bool_(false);
        }
        // Get the TSWTypeMeta from _meta (stored in PyTimeSeriesType base class)
        if (!_meta) {
            return nb::bool_(v.valid());  // Fallback to valid()
        }
        if (_meta->ts_kind != TSKind::TSW) {
            return nb::bool_(v.valid());  // Fallback to valid()
        }
        auto* tsw_meta = static_cast<const TSWTypeMeta*>(_meta);
        int64_t min_size = tsw_meta->min_size;
        if (min_size <= 0) {
            // No min_size constraint, valid is sufficient
            return nb::bool_(v.valid());
        }
        // Get the current window size
        auto* storage = static_cast<const value::WindowStorage*>(v.value_view().data());
        if (!storage) {
            return nb::bool_(false);
        }
        size_t current_size = storage->size();
        return nb::bool_(current_size >= static_cast<size_t>(min_size));
    }

    nb::int_ PyTimeSeriesWindowInput::len() const {
        return nb::int_(view().window_size());
    }

    nb::str PyTimeSeriesWindowInput::py_str() const {
        return nb::str("TSW[...]");
    }

    nb::str PyTimeSeriesWindowInput::py_repr() const {
        return py_str();
    }

    void tsw_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowOutput, PyTimeSeriesOutput>(m, "TimeSeriesWindowOutput")
            .def_prop_ro("value_times", &PyTimeSeriesWindowOutput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesWindowOutput::first_modified_time)
            .def_prop_ro("size", &PyTimeSeriesWindowOutput::window_size)
            .def_prop_ro("min_size", &PyTimeSeriesWindowOutput::min_size)
            .def_prop_ro("has_removed_value", &PyTimeSeriesWindowOutput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesWindowOutput::removed_value)
            .def("__len__", &PyTimeSeriesWindowOutput::len)
            .def("__str__", &PyTimeSeriesWindowOutput::py_str)
            .def("__repr__", &PyTimeSeriesWindowOutput::py_repr);

        nb::class_<PyTimeSeriesWindowInput, PyTimeSeriesInput>(m, "TimeSeriesWindowInput")
            .def_prop_ro("value_times", &PyTimeSeriesWindowInput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesWindowInput::first_modified_time)
            .def_prop_ro("has_removed_value", &PyTimeSeriesWindowInput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesWindowInput::removed_value)
            .def_prop_ro("all_valid", &PyTimeSeriesWindowInput::all_valid)
            .def("__len__", &PyTimeSeriesWindowInput::len)
            .def("__str__", &PyTimeSeriesWindowInput::py_str)
            .def("__repr__", &PyTimeSeriesWindowInput::py_repr);
    }

}  // namespace hgraph
