#ifndef HGRAPH_TYPES_METADATA_DETAIL_TS_DATA_SEAMS_H
#define HGRAPH_TYPES_METADATA_DETAIL_TS_DATA_SEAMS_H

#include <hgraph/types/python_object.h>
#include <hgraph/types/time_series/ts_data/types.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <cstdint>

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

    // -- fixed structured TSB / TSL -------------------------------------------------
    // Children are reached through their own erased TSDataOps
    // (``fixed_element_type(...).ops_ref()``); the seams answer the shape.
    [[nodiscard]] const TSValueTypeMetaData &fixed_schema(const void *context) noexcept;
    [[nodiscard]] const TSDataLayout &fixed_layout(const void *context) noexcept;
    [[nodiscard]] std::size_t fixed_element_count(const void *context) noexcept;
    [[nodiscard]] TSRoleTypeRef fixed_element_type(const void *context, std::size_t index) noexcept;
    [[nodiscard]] std::int64_t fixed_ordinal_key(const void *context, std::size_t index) noexcept;
    [[nodiscard]] const TSDataTracking &fixed_tracking(const void *context, const void *memory) noexcept;
    [[nodiscard]] const void *fixed_child_data(const void *context, const void *memory, std::size_t index) noexcept;
    [[nodiscard]] void *fixed_mutable_child_data(const void *context, void *memory, std::size_t index) noexcept;
    [[nodiscard]] bool fixed_child_modified_for_parent_time(const void *context, const void *memory,
                                                            std::size_t index) noexcept;

    // -- dynamic TSL -------------------------------------------------------------------
    [[nodiscard]] const FixedTSLDataLayout &dynamic_layout(const void *context) noexcept;
    [[nodiscard]] TSRoleTypeRef dynamic_element_type(const void *context) noexcept;
    [[nodiscard]] ValueTypeRef dynamic_delta_key_set_binding(const void *context) noexcept;
    [[nodiscard]] ValueTypeRef dynamic_removed_set_binding(const void *context) noexcept;
    [[nodiscard]] ValueTypeRef dynamic_modified_map_binding(const void *context) noexcept;
    [[nodiscard]] const TSDataTracking &dynamic_tracking(const void *memory) noexcept;
    [[nodiscard]] std::size_t dynamic_size(const void *memory) noexcept;
    [[nodiscard]] const void *dynamic_child_memory(const void *memory, std::size_t index);
    [[nodiscard]] void *dynamic_mutable_child_memory(void *memory, std::size_t index);
    [[nodiscard]] std::size_t dynamic_modified_index_count(const void *memory) noexcept;
    [[nodiscard]] std::size_t dynamic_modified_index_at(const void *memory, std::size_t ordinal);
    void dynamic_ensure_size(const void *context, void *memory, std::size_t size, DateTime modified_time);
    void dynamic_resize(const void *context, void *memory, std::size_t size, DateTime modified_time);
    void dynamic_record_child_modified(void *memory, std::size_t index, DateTime modified_time);

    // -- slot-backed TSS / TSD ---------------------------------------------------------
    enum class SetSurface : std::uint8_t
    {
        live,
        added,
        removed,
    };
    enum class MapSurface : std::uint8_t
    {
        live,
        modified,
    };
    struct SlotInsert
    {
        std::size_t slot{0};
        bool        changed{false};
    };

    [[nodiscard]] const TSSDataLayout &tss_layout(const void *context) noexcept;
    [[nodiscard]] ValueTypeRef tss_added_set_binding(const void *context) noexcept;
    [[nodiscard]] ValueTypeRef tss_removed_set_binding(const void *context) noexcept;
    [[nodiscard]] const TSDataTracking &tss_tracking(const void *memory) noexcept;
    [[nodiscard]] Range<ValueView> tss_keys(const void *context, const void *memory, SetSurface surface);
    [[nodiscard]] bool tss_touch(void *memory, DateTime modified_time);
    void tss_insert_key(void *memory, const ValueView &key, DateTime modified_time);
    void tss_remove_key(void *memory, const ValueView &key, DateTime modified_time);

    [[nodiscard]] const TSDDataLayout &tsd_layout(const void *context) noexcept;
    [[nodiscard]] ValueTypeRef tsd_removed_set_binding(const void *context) noexcept;
    [[nodiscard]] ValueTypeRef tsd_modified_map_binding(const void *context) noexcept;
    [[nodiscard]] const TSDataTracking &tsd_tracking(const void *memory) noexcept;
    [[nodiscard]] std::size_t tsd_slot_capacity(const void *memory) noexcept;
    [[nodiscard]] bool tsd_slot_in_surface(const void *memory, std::size_t slot, MapSurface surface) noexcept;
    [[nodiscard]] const void *tsd_key_at_slot(const void *memory, std::size_t slot);
    [[nodiscard]] const void *tsd_child_at_slot(const void *memory, std::size_t slot);
    [[nodiscard]] Range<ValueView> tsd_keys(const void *context, const void *memory, SetSurface surface);
    /** The map surfaces the value ops project: (key, value-or-invalid) pairs. */
    [[nodiscard]] ValueTypeRef tsd_map_value_binding(const void *context, const void *memory, MapSurface surface) noexcept;
    [[nodiscard]] KeyValueRange<ValueView, ValueView> tsd_map_items(const void *context, const void *memory,
                                                                     MapSurface surface);
    [[nodiscard]] bool tsd_touch(void *memory, DateTime modified_time);
    [[nodiscard]] SlotInsert tsd_insert_key(void *memory, const ValueView &key, DateTime modified_time);
    void tsd_remove_key(void *memory, const ValueView &key, DateTime modified_time);
    [[nodiscard]] void *tsd_child_memory_for_write(void *memory, std::size_t slot);
    void tsd_record_child_modified(void *memory, std::size_t slot, DateTime modified_time);

    // -- TSD proxy (an input-side projection over a bound TSD) ---------------------
    enum class ProxyMapSurface : std::uint8_t
    {
        live,
        added,
        removed,
        modified,
    };
    [[nodiscard]] const TSDDataLayout &proxy_layout(const void *context) noexcept;
    [[nodiscard]] const TSDataTracking &proxy_tracking(const void *memory) noexcept;
    [[nodiscard]] const TSDataTracking &proxy_key_set_tracking(const void *memory) noexcept;
    /** Both read the bound source and throw when the proxy is unbound. */
    [[nodiscard]] std::size_t proxy_slot_capacity(const void *memory);
    [[nodiscard]] bool proxy_slot_live(const void *memory, std::size_t slot);
    [[nodiscard]] bool proxy_slot_modified(const void *context, const void *memory, std::size_t slot);
    [[nodiscard]] bool proxy_has_child(const void *memory, std::size_t slot) noexcept;
    [[nodiscard]] const void *proxy_child_at_slot(const void *memory, std::size_t slot);
    [[nodiscard]] ValueView proxy_key_at_slot(const void *memory, std::size_t slot);
    // The surface is a template argument, as it is on the proxy's own
    // surface templates: a conversion body instantiated per surface calls
    // the already-selected operation, with no switch inside its slot loop.
    // The instantiations live in proxy.cpp (one per surface value).
    template <SetSurface Surface>
    [[nodiscard]] Range<ValueView> proxy_keys(const void *context, const void *memory);
    template <SetSurface Surface>
    [[nodiscard]] bool proxy_slot_in_set_surface(const void *context, const void *memory, std::size_t slot);
    template <ProxyMapSurface Surface>
    [[nodiscard]] bool proxy_slot_in_map_surface(const void *context, const void *memory, std::size_t slot);
    template <ProxyMapSurface Surface>
    [[nodiscard]] ValueTypeRef proxy_map_value_binding(const void *context, const void *memory) noexcept;
    template <ProxyMapSurface Surface>
    [[nodiscard]] const void *proxy_map_value_at_slot(const void *context, const void *memory, std::size_t slot);
}  // namespace hgraph::ts_data_seams

#endif  // HGRAPH_TYPES_METADATA_DETAIL_TS_DATA_SEAMS_H
