#include <hgraph/api/python/py_tsw.h>
#include <hgraph/types/time_series/ts_window_view.h>

namespace hgraph
{
    // ========== PyTimeSeriesFixedWindowOutput ==========

    PyTimeSeriesFixedWindowOutput::PyTimeSeriesFixedWindowOutput(api_ptr impl)
        : PyTimeSeriesOutput(std::move(impl)) {}

    PyTimeSeriesFixedWindowOutput::PyTimeSeriesFixedWindowOutput(TSOutputView view)
        : PyTimeSeriesOutput(std::move(view)) {}

    TimeSeriesFixedWindowOutput* PyTimeSeriesFixedWindowOutput::impl() const {
        return this->template static_cast_impl<TimeSeriesFixedWindowOutput>();
    }

    nb::object PyTimeSeriesFixedWindowOutput::value_times() const {
        if (has_output_view()) {
            auto wv = output_view().ts_view().as_window();
            const engine_time_t* times = wv.value_times();
            size_t count = wv.value_times_count();
            if (!times || count == 0) return nb::none();
            std::vector<engine_time_t> result(times, times + count);
            return nb::cast(result);
        }
        return impl()->py_value_times();
    }

    engine_time_t PyTimeSeriesFixedWindowOutput::first_modified_time() const {
        if (has_output_view()) {
            return output_view().ts_view().as_window().first_modified_time();
        }
        return impl()->first_modified_time();
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::size() const {
        if (has_output_view()) {
            return nb::int_(output_view().ts_view().as_window().window_size());
        }
        return nb::int_(impl()->size());
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::min_size() const {
        if (has_output_view()) {
            return nb::int_(output_view().ts_view().as_window().min_window_size());
        }
        return nb::int_(impl()->min_size());
    }

    nb::bool_ PyTimeSeriesFixedWindowOutput::has_removed_value() const {
        if (has_output_view()) {
            return nb::bool_(output_view().ts_view().as_window().has_removed_value());
        }
        return nb::bool_(impl()->has_removed_value());
    }

    nb::object PyTimeSeriesFixedWindowOutput::removed_value() const {
        if (has_output_view()) {
            auto wv = output_view().ts_view().as_window();
            if (!wv.has_removed_value()) return nb::none();
            value::View rv = wv.removed_value();
            if (!rv.valid()) return nb::none();
            return rv.to_python();
        }
        return impl()->py_removed_value();
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::len() const {
        if (has_output_view()) {
            return nb::int_(output_view().ts_view().as_window().length());
        }
        return nb::int_(impl()->len());
    }

    // ========== PyTimeSeriesTimeWindowOutput ==========

    PyTimeSeriesTimeWindowOutput::PyTimeSeriesTimeWindowOutput(api_ptr impl)
        : PyTimeSeriesOutput(std::move(impl)) {}

    PyTimeSeriesTimeWindowOutput::PyTimeSeriesTimeWindowOutput(TSOutputView view)
        : PyTimeSeriesOutput(std::move(view)) {}

    TimeSeriesTimeWindowOutput* PyTimeSeriesTimeWindowOutput::impl() const {
        return this->template static_cast_impl<TimeSeriesTimeWindowOutput>();
    }

    nb::object PyTimeSeriesTimeWindowOutput::value_times() const {
        if (has_output_view()) {
            auto wv = output_view().ts_view().as_window();
            const engine_time_t* times = wv.value_times();
            size_t count = wv.value_times_count();
            if (!times || count == 0) return nb::none();
            std::vector<engine_time_t> result(times, times + count);
            return nb::cast(result);
        }
        return impl()->py_value_times();
    }

    engine_time_t PyTimeSeriesTimeWindowOutput::first_modified_time() const {
        if (has_output_view()) {
            return output_view().ts_view().as_window().first_modified_time();
        }
        return impl()->first_modified_time();
    }

    nb::object PyTimeSeriesTimeWindowOutput::size() const {
        if (has_output_view()) {
            // For time windows, return the duration as timedelta
            size_t micros = output_view().ts_view().as_window().window_size();
            return nb::cast(engine_time_delta_t{static_cast<int64_t>(micros)});
        }
        return nb::cast(impl()->size());
    }

    nb::object PyTimeSeriesTimeWindowOutput::min_size() const {
        if (has_output_view()) {
            // For time windows, return the duration as timedelta
            size_t micros = output_view().ts_view().as_window().min_window_size();
            return nb::cast(engine_time_delta_t{static_cast<int64_t>(micros)});
        }
        return nb::cast(impl()->min_size());
    }

    nb::bool_ PyTimeSeriesTimeWindowOutput::has_removed_value() const {
        if (has_output_view()) {
            return nb::bool_(output_view().ts_view().as_window().has_removed_value());
        }
        return nb::bool_(impl()->has_removed_value());
    }

    nb::object PyTimeSeriesTimeWindowOutput::removed_value() const {
        if (has_output_view()) {
            auto wv = output_view().ts_view().as_window();
            if (!wv.has_removed_value()) return nb::none();
            // For time windows, access the removed value pointers directly
            const auto* removed_ptrs = wv.removed_value_ptrs();
            if (!removed_ptrs || removed_ptrs->empty()) return nb::none();
            // Convert to Python list
            nb::list result;
            const auto* elem_type = wv.meta()->value_type;
            for (void* ptr : *removed_ptrs) {
                if (ptr) {
                    value::View elem_view(ptr, elem_type);
                    result.append(elem_view.to_python());
                }
            }
            return result;
        }
        return impl()->py_removed_value();
    }

    nb::int_ PyTimeSeriesTimeWindowOutput::len() const {
        if (has_output_view()) {
            return nb::int_(output_view().ts_view().as_window().length());
        }
        return nb::int_(impl()->len());
    }

    // ========== PyTimeSeriesWindowInput ==========

    PyTimeSeriesWindowInput::PyTimeSeriesWindowInput(api_ptr impl)
        : PyTimeSeriesInput(std::move(impl)) {}

    PyTimeSeriesWindowInput::PyTimeSeriesWindowInput(TSInputView view)
        : PyTimeSeriesInput(std::move(view)) {}

    TimeSeriesWindowInput* PyTimeSeriesWindowInput::impl() const {
        return this->template static_cast_impl<TimeSeriesWindowInput>();
    }

    nb::object PyTimeSeriesWindowInput::value_times() const {
        if (has_input_view()) {
            auto wv = input_view().ts_view().as_window();
            const engine_time_t* times = wv.value_times();
            size_t count = wv.value_times_count();
            if (!times || count == 0) return nb::none();
            std::vector<engine_time_t> result(times, times + count);
            return nb::cast(result);
        }
        return impl()->py_value_times();
    }

    engine_time_t PyTimeSeriesWindowInput::first_modified_time() const {
        if (has_input_view()) {
            return input_view().ts_view().as_window().first_modified_time();
        }
        return impl()->first_modified_time();
    }

    nb::bool_ PyTimeSeriesWindowInput::has_removed_value() const {
        if (has_input_view()) {
            return nb::bool_(input_view().ts_view().as_window().has_removed_value());
        }
        return nb::bool_(impl()->has_removed_value());
    }

    nb::object PyTimeSeriesWindowInput::removed_value() const {
        if (has_input_view()) {
            auto wv = input_view().ts_view().as_window();
            if (!wv.has_removed_value()) return nb::none();
            // Check if it's a time window (multiple removed values) or fixed (single)
            if (wv.is_duration_based()) {
                // Time window - access the removed value pointers directly
                const auto* removed_ptrs = wv.removed_value_ptrs();
                if (!removed_ptrs || removed_ptrs->empty()) return nb::none();
                // Convert to Python list
                nb::list result;
                const auto* elem_type = wv.meta()->value_type;
                for (void* ptr : *removed_ptrs) {
                    if (ptr) {
                        value::View elem_view(ptr, elem_type);
                        result.append(elem_view.to_python());
                    }
                }
                return result;
            }
            value::View rv = wv.removed_value();
            if (!rv.valid()) return nb::none();
            return rv.to_python();
        }
        return impl()->py_removed_value();
    }

    nb::int_ PyTimeSeriesWindowInput::len() const {
        if (has_input_view()) {
            return nb::int_(input_view().ts_view().as_window().length());
        }
        return nb::int_(impl()->len());
    }

    // ========== Nanobind Registration ==========

    void tsw_register_with_nanobind(nb::module_ &m) {
        // Fixed-size window output
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
        nb::class_<PyTimeSeriesWindowInput, PyTimeSeriesInput>(m, "TimeSeriesWindowInput")
            .def_prop_ro("value_times", &PyTimeSeriesWindowInput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesWindowInput::first_modified_time)
            .def_prop_ro("has_removed_value", &PyTimeSeriesWindowInput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesWindowInput::removed_value)
            .def("__len__", &PyTimeSeriesWindowInput::len);
    }

}  // namespace hgraph
