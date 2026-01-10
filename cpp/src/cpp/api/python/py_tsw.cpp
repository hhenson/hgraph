#include <hgraph/api/python/py_tsw.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/value/window_storage_ops.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>

namespace hgraph
{
    // ========== Helper to get WindowStorage from view ==========

    static const value::WindowStorage* get_window_storage_const(const TSView& view) {
        return static_cast<const value::WindowStorage*>(view.value_view().data());
    }

    static const TSWTypeMeta* get_tsw_meta(const TSView& view) {
        return static_cast<const TSWTypeMeta*>(view.ts_meta());
    }

    // ========== PyTimeSeriesFixedWindowOutput ==========
    //
    // Note: value(), delta_value(), all_valid() are inherited from PyTimeSeriesOutput
    // and work correctly through the view/ops layer (WindowStorageOps).

    PyTimeSeriesFixedWindowOutput::PyTimeSeriesFixedWindowOutput(TSMutableView view)
        : PyTimeSeriesOutput(view) {}

    nb::object PyTimeSeriesFixedWindowOutput::value_times() const {
        const value::TypeMeta* schema = _view.ts_meta()->value_schema();
        return value::WindowStorageOps::get_times_python(_view.value_view().data(), schema);
    }

    engine_time_t PyTimeSeriesFixedWindowOutput::first_modified_time() const {
        const value::TypeMeta* schema = _view.ts_meta()->value_schema();
        return value::WindowStorageOps::get_oldest_time(_view.value_view().data(), schema);
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::size() const {
        const TSWTypeMeta* tsw_meta = get_tsw_meta(_view);
        return nb::int_(tsw_meta->size());
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::min_size() const {
        const TSWTypeMeta* tsw_meta = get_tsw_meta(_view);
        return nb::int_(tsw_meta->min_size());
    }

    nb::bool_ PyTimeSeriesFixedWindowOutput::has_removed_value() const {
        const value::TypeMeta* schema = _view.ts_meta()->value_schema();
        return nb::bool_(value::WindowStorageOps::has_removed_value(_view.value_view().data(), schema));
    }

    nb::object PyTimeSeriesFixedWindowOutput::removed_value() const {
        const value::TypeMeta* schema = _view.ts_meta()->value_schema();
        return value::WindowStorageOps::get_removed_value_python(_view.value_view().data(), schema);
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::len() const {
        const value::WindowStorage* storage = get_window_storage_const(_view);
        return nb::int_(storage->size);
    }

    // ========== PyTimeSeriesTimeWindowOutput ==========

    PyTimeSeriesTimeWindowOutput::PyTimeSeriesTimeWindowOutput(TSMutableView view)
        : PyTimeSeriesOutput(view) {}

    nb::object PyTimeSeriesTimeWindowOutput::value_times() const {
        // Time-delta windows use Queue storage, not Window - use different accessors
        // For now, throw not implemented
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::value_times not yet implemented for time-delta windows");
    }

    engine_time_t PyTimeSeriesTimeWindowOutput::first_modified_time() const {
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::first_modified_time not yet implemented for time-delta windows");
    }

    nb::object PyTimeSeriesTimeWindowOutput::size() const {
        const TSWTypeMeta* tsw_meta = get_tsw_meta(_view);
        // Return timedelta for time-based windows
        return nb::cast(tsw_meta->time_range());
    }

    nb::object PyTimeSeriesTimeWindowOutput::min_size() const {
        const TSWTypeMeta* tsw_meta = get_tsw_meta(_view);
        // Return timedelta for time-based windows
        return nb::cast(tsw_meta->min_time_range());
    }

    nb::bool_ PyTimeSeriesTimeWindowOutput::has_removed_value() const {
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::has_removed_value not yet implemented for time-delta windows");
    }

    nb::object PyTimeSeriesTimeWindowOutput::removed_value() const {
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::removed_value not yet implemented for time-delta windows");
    }

    nb::int_ PyTimeSeriesTimeWindowOutput::len() const {
        throw std::runtime_error("PyTimeSeriesTimeWindowOutput::len not yet implemented for time-delta windows");
    }

    // ========== PyTimeSeriesWindowInput ==========
    //
    // Note: value(), delta_value(), all_valid() are inherited from PyTimeSeriesInput
    // and work correctly through the view/ops layer (WindowStorageOps).

    PyTimeSeriesWindowInput::PyTimeSeriesWindowInput(TSView view)
        : PyTimeSeriesInput(view) {}

    nb::object PyTimeSeriesWindowInput::value_times() const {
        const value::TypeMeta* schema = _view.ts_meta()->value_schema();
        if (schema->kind == value::TypeKind::Window) {
            return value::WindowStorageOps::get_times_python(_view.value_view().data(), schema);
        }
        // Time-delta windows
        throw std::runtime_error("PyTimeSeriesWindowInput::value_times not yet implemented for time-delta windows");
    }

    engine_time_t PyTimeSeriesWindowInput::first_modified_time() const {
        const value::TypeMeta* schema = _view.ts_meta()->value_schema();
        if (schema->kind == value::TypeKind::Window) {
            return value::WindowStorageOps::get_oldest_time(_view.value_view().data(), schema);
        }
        throw std::runtime_error("PyTimeSeriesWindowInput::first_modified_time not yet implemented for time-delta windows");
    }

    nb::bool_ PyTimeSeriesWindowInput::has_removed_value() const {
        const value::TypeMeta* schema = _view.ts_meta()->value_schema();
        if (schema->kind == value::TypeKind::Window) {
            return nb::bool_(value::WindowStorageOps::has_removed_value(_view.value_view().data(), schema));
        }
        throw std::runtime_error("PyTimeSeriesWindowInput::has_removed_value not yet implemented for time-delta windows");
    }

    nb::object PyTimeSeriesWindowInput::removed_value() const {
        const value::TypeMeta* schema = _view.ts_meta()->value_schema();
        if (schema->kind == value::TypeKind::Window) {
            return value::WindowStorageOps::get_removed_value_python(_view.value_view().data(), schema);
        }
        throw std::runtime_error("PyTimeSeriesWindowInput::removed_value not yet implemented for time-delta windows");
    }

    nb::int_ PyTimeSeriesWindowInput::len() const {
        const value::TypeMeta* schema = _view.ts_meta()->value_schema();
        if (schema->kind == value::TypeKind::Window) {
            const value::WindowStorage* storage = get_window_storage_const(_view);
            return nb::int_(storage->size);
        }
        throw std::runtime_error("PyTimeSeriesWindowInput::len not yet implemented for time-delta windows");
    }

    // ========== Nanobind Registration ==========

    void tsw_register_with_nanobind(nb::module_ &m) {
        // Fixed-size window output
        // Note: value, delta_value, all_valid are inherited from base class
        nb::class_<PyTimeSeriesFixedWindowOutput, PyTimeSeriesOutput>(m, "TimeSeriesFixedWindowOutput")
            .def_prop_ro("value_times", &PyTimeSeriesFixedWindowOutput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesFixedWindowOutput::first_modified_time)
            .def_prop_ro("size", &PyTimeSeriesFixedWindowOutput::size)
            .def_prop_ro("min_size", &PyTimeSeriesFixedWindowOutput::min_size)
            .def_prop_ro("has_removed_value", &PyTimeSeriesFixedWindowOutput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesFixedWindowOutput::removed_value)
            .def("__len__", &PyTimeSeriesFixedWindowOutput::len);

        // Time-based window output
        nb::class_<PyTimeSeriesTimeWindowOutput, PyTimeSeriesOutput>(m, "TimeSeriesTimeWindowOutput")
            .def_prop_ro("value_times", &PyTimeSeriesTimeWindowOutput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesTimeWindowOutput::first_modified_time)
            .def_prop_ro("size", &PyTimeSeriesTimeWindowOutput::size)
            .def_prop_ro("min_size", &PyTimeSeriesTimeWindowOutput::min_size)
            .def_prop_ro("has_removed_value", &PyTimeSeriesTimeWindowOutput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesTimeWindowOutput::removed_value)
            .def("__len__", &PyTimeSeriesTimeWindowOutput::len);

        // Window input (works with both fixed and time-based outputs)
        // Note: value, delta_value, all_valid are inherited from base class
        nb::class_<PyTimeSeriesWindowInput, PyTimeSeriesInput>(m, "TimeSeriesWindowInput")
            .def_prop_ro("value_times", &PyTimeSeriesWindowInput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesWindowInput::first_modified_time)
            .def_prop_ro("has_removed_value", &PyTimeSeriesWindowInput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesWindowInput::removed_value)
            .def("__len__", &PyTimeSeriesWindowInput::len);
    }

}  // namespace hgraph
