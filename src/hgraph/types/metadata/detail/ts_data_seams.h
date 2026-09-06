#ifndef HGRAPH_TYPES_METADATA_DETAIL_TS_DATA_SEAMS_H
#define HGRAPH_TYPES_METADATA_DETAIL_TS_DATA_SEAMS_H

#include <hgraph/types/python_object.h>
#include <hgraph/types/time_series/ts_data/types.h>
#include <hgraph/util/date_time.h>

#include <cstddef>

/**
 * Private seams of the TSData families (RFC 0035, PR 4): what the bridge's
 * ``ts_data_family_conversions.cpp`` needs from the atomic and window
 * strategies, with no Python in it. Memory arithmetic and the storages'
 * mutation protocol stay here; the bridge converts into the payloads the
 * seams hand it. Private to ``hgraph_runtime``.
 */
namespace hgraph::ts_data_seams
{
    // -- atomic TS / SIGNAL / REF -----------------------------------------------
    // ``context`` is the strategy's layout (its first member), so the public
    // ``TSDataLayout`` answers bindings and offsets; the retained-object holder
    // is the one private field.
    [[nodiscard]] const TSDataLayout &atomic_layout(const void *context) noexcept;
    [[nodiscard]] const TSDataTracking &atomic_tracking(const void *context, const void *memory) noexcept;
    [[nodiscard]] const void *atomic_value_memory(const void *context, const void *memory) noexcept;
    [[nodiscard]] void *atomic_mutable_value_memory(const void *context, void *memory) noexcept;
    [[nodiscard]] const void *atomic_delta_memory(const void *context, const void *memory) noexcept;
    /** The retained-object holder of a ``NativeWithPythonCache`` output, or null. */
    [[nodiscard]] void *atomic_retained_holder(const void *context, void *memory) noexcept;

    // -- TSW windows --------------------------------------------------------------
    [[nodiscard]] const TSWDataLayout &window_layout(const void *context) noexcept;
    [[nodiscard]] const TSDataTracking &window_tracking(const void *context, const void *memory) noexcept;
    [[nodiscard]] const void *window_value_memory(const void *context, const void *memory) noexcept;
    /** The delta element memory for the current cycle, or null when none. */
    [[nodiscard]] const void *window_delta_memory(const void *context, const void *memory) noexcept;
    /** Over the window STORAGE (the value memory), the shape the value ops see. */
    [[nodiscard]] std::size_t window_storage_size(const void *context, const void *storage) noexcept;
    [[nodiscard]] const void *window_storage_element_at(const void *context, const void *storage,
                                                        std::size_t index) noexcept;

    /** Convert element ``index`` into ``payload``, constructed as ``element_binding``. */
    using FillElementFn = void (*)(void *fill_context, std::size_t index, ValueTypeRef element_binding, void *payload);

    /** Replace the window's contents with ``count`` elements (a size window
        rejects more than its period), each pushed at ``modified_time``. */
    void window_replace(const void *context, void *memory, DateTime modified_time, std::size_t count,
                        FillElementFn fill, void *fill_context);
    /** Push one element at ``modified_time``. */
    void window_push(const void *context, void *memory, DateTime modified_time, FillElementFn fill, void *fill_context);
}  // namespace hgraph::ts_data_seams

#endif  // HGRAPH_TYPES_METADATA_DETAIL_TS_DATA_SEAMS_H
