//
// Python helper functions for ts::TSOutput and ts::TSInput
//
// These functions provide Python-aware operations for value-based time-series types.
// They delegate conversion logic to the schema's from_python/to_python ops.
//
// Pattern:
//   apply_result(value): If None, do nothing. Otherwise call set_value(value).
//   set_value(value): If None, invalidate. Otherwise convert and set.
//

#ifndef HGRAPH_TS_PYTHON_HELPERS_H
#define HGRAPH_TS_PYTHON_HELPERS_H

#include <nanobind/nanobind.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/value/python_conversion.h>

namespace hgraph::ts
{

/**
 * Set a Python value on a TSOutput, using the schema's from_python conversion.
 *
 * If py_value is None, the output is invalidated.
 * Otherwise, the value is converted using the schema's ops->from_python.
 *
 * @param output The output to set
 * @param py_value The Python value to set
 * @param time The evaluation time for marking modification
 */
inline void set_python_value(TSOutput* output, nb::object py_value, engine_time_t time) {
    if (!output) return;

    // None means invalidate
    if (py_value.is_none()) {
        output->mark_invalid();
        return;
    }

    auto view = output->view();
    auto ts_value_view = view.value_view();
    auto* schema = ts_value_view.schema();

    if (schema && schema->ops && schema->ops->from_python) {
        // Get the underlying ValueView which has the data() method
        auto value_view = ts_value_view.value_view();
        schema->ops->from_python(value_view.data(), py_value.ptr(), schema);
        view.mark_modified(time);
    }
}

/**
 * Apply a Python result to a TSOutput.
 *
 * This is the main entry point for setting a value from Python.
 * If py_value is None, this does nothing (returns immediately).
 * Otherwise, it calls set_python_value to do the conversion.
 *
 * @param output The output to apply to
 * @param py_value The Python value to apply
 * @param time The evaluation time for marking modification
 */
inline void apply_python_result(TSOutput* output, nb::object py_value, engine_time_t time) {
    if (!output) return;

    // None means "no result" - do nothing
    if (py_value.is_none()) return;

    set_python_value(output, std::move(py_value), time);
}

/**
 * Check if a Python value can be applied to the output.
 *
 * For simple values this always returns true if the output is valid.
 * Collection types may override this with more specific checks.
 *
 * @param output The output to check
 * @param py_value The Python value to check
 * @return true if the value can be applied
 */
inline bool can_apply_python_result(TSOutput* output, nb::object py_value) {
    if (!output) return false;
    // For now, we can always apply if the output exists
    // More sophisticated checks could be added based on schema
    (void)py_value;
    return true;
}

/**
 * Get the Python value from a TSOutput.
 *
 * Uses the schema's to_python conversion.
 *
 * @param output The output to get from
 * @return The Python object, or None if not valid
 */
inline nb::object get_python_value(const TSOutput* output) {
    if (!output || !output->has_value()) return nb::none();

    auto view = const_cast<TSOutput*>(output)->view();
    auto ts_value_view = view.value_view();
    auto* schema = ts_value_view.schema();

    if (!ts_value_view.valid() || !schema) return nb::none();

    // Get the underlying ValueView which has the data() method
    auto value_view = ts_value_view.value_view();
    return value::value_to_python(value_view.data(), schema);
}

/**
 * Get the Python value from a TSInput.
 *
 * Uses the schema's to_python conversion.
 *
 * @param input The input to get from
 * @return The Python object, or None if not valid
 */
inline nb::object get_python_value(const TSInput* input) {
    if (!input || !input->has_value()) return nb::none();

    auto view = input->view();
    // TSInputView::value_view() returns ConstValueView directly (not TimeSeriesValueView)
    auto& value_view = view.value_view();
    auto* schema = value_view.schema();

    if (!value_view.valid() || !schema) return nb::none();

    return value::value_to_python(value_view.data(), schema);
}

} // namespace hgraph::ts

#endif // HGRAPH_TS_PYTHON_HELPERS_H
