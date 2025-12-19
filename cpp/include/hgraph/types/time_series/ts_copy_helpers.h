//
// Type-erased copy helpers for time-series outputs and inputs
//
// These functions provide type-erased copying between time-series outputs and views.
// They use TypeMeta::copy_assign_at for efficient, type-safe copying without Python dependencies.
//

#ifndef HGRAPH_TS_COPY_HELPERS_H
#define HGRAPH_TS_COPY_HELPERS_H

#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph::ts
{

/**
 * Copy value from a ConstValueView to a TSOutput.
 *
 * Uses TypeMeta::copy_assign_at for type-erased copy.
 * This is the core copy function - other copy functions delegate to this.
 *
 * @param output The destination output
 * @param source The source view (data + schema)
 * @param time Engine time for marking modification
 * @return true if copy succeeded, false if schema mismatch or invalid input
 */
inline bool copy_from_view(TSOutput* output, const value::ConstValueView& source, engine_time_t time) {
    if (!output || !source.valid()) return false;

    auto view = output->view();
    auto ts_value_view = view.value_view();
    auto* dest_schema = ts_value_view.schema();

    // Schema compatibility check (exact match required)
    if (dest_schema != source.schema()) {
        return false;
    }

    // Type-erased copy via TypeOps
    auto dest_view = ts_value_view.value_view();
    dest_schema->copy_assign_at(dest_view.data(), source.data());
    view.mark_modified(time);
    return true;
}

/**
 * Copy value from a TSInputView to a TSOutput.
 *
 * Convenience wrapper that extracts the underlying ConstValueView from the input view.
 *
 * @param output The destination output
 * @param source The source input view
 * @param time Engine time for marking modification
 * @return true if copy succeeded, false if schema mismatch or invalid input
 */
inline bool copy_from_input_view(TSOutput* output, const TSInputView& source, engine_time_t time) {
    if (!source.valid()) return false;
    return copy_from_view(output, source.value_view(), time);
}

/**
 * Copy value from a TSOutputView to a TSOutput.
 *
 * Convenience wrapper for output-to-output copy.
 *
 * @param output The destination output
 * @param source The source output view
 * @param time Engine time for marking modification
 * @return true if copy succeeded, false if schema mismatch or invalid input
 */
inline bool copy_from_output_view(TSOutput* output, const TSOutputView& source, engine_time_t time) {
    if (!source.valid()) return false;
    // TSOutputView::value_view() returns TimeSeriesValueView, which has value_view() returning ValueView
    // We need ConstValueView, so we use the const accessor
    auto ts_value_view = source.value_view();
    auto value_view = ts_value_view.value_view();
    // Create ConstValueView from ValueView
    return copy_from_view(output, value::ConstValueView(value_view.data(), value_view.schema()), time);
}

} // namespace hgraph::ts

#endif // HGRAPH_TS_COPY_HELPERS_H
