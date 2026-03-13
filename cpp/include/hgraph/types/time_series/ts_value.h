#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_value_builder.h>

#include <functional>

namespace hgraph {

/**
 * Shared owning storage for a logical time-series value.
 *
 * `TSValue` is intended to hold endpoint-local time-series storage in the
 * same broad shape as the new `Value` layer:
 * - one cached schema-bound builder
 * - one owned storage block
 * - non-owning views built over that storage
 *
 * The owned storage block is divided into two aligned regions:
 * - a value region described by the value-layer `ValueBuilder`
 * - a time-series extension region described by `TSStateOps`
 *
 * The value region is laid out first so fixed shapes preserve their
 * data-first representation and remain suitable for vectorised access. The TS
 * region follows that at the required alignment boundary and carries the
 * additional modification/subscription/link state for the same logical value.
 *
 * This is the shared storage contract used by both `TSInput` and `TSOutput`.
 * Endpoint-specific concerns such as input active-state tracking and output
 * alternative representations build on top of this base storage.
 */
struct HGRAPH_EXPORT TSValue {
    /**
     * Construct time-series value storage.
     *
     * Time-series value storage is schema-bound. The supplied time-series
     * schema defines both the time-series behavior and the underlying value
     * schema used by the owned `Value`.
     */
    explicit TSValue(const TSMeta &schema);
    TSValue(const TSValue &other);
    TSValue(TSValue &&other) noexcept;
    TSValue &operator=(const TSValue &other);
    TSValue &operator=(TSValue &&other) noexcept;
    ~TSValue();

protected:
    /**
     * Return the logical time-series schema satisfied by this storage.
     */
    [[nodiscard]] const TSMeta &schema() const noexcept { return m_schema.get(); }

    /**
     * Return the stored endpoint value as a read-only erased view.
     */
    [[nodiscard]] View value() const noexcept;

    /**
     * Return the stored endpoint value as a mutable erased view.
     */
    [[nodiscard]] View value() noexcept;

    /**
     * Return the scalar-like value surface for this time-series.
     *
     * Scalar time-series, references, signals, and windows use their live
     * value surface directly. Collection kinds expose their current value
     * through the collection-specific helpers below.
     */
    [[nodiscard]] AtomicView atomic_value() const;

    /**
     * Return the list value surface for this time-series.
     */
    [[nodiscard]] ListView list_value() const;

    /**
     * Return the bundle value surface for this time-series.
     */
    [[nodiscard]] BundleView bundle_value() const;

    /**
     * Return the set value surface for this time-series.
     *
     * The stored value is built with delta tracking, so the returned set view
     * retains stable-slot semantics needed by the time-series layer. Removed
     * payloads remain inspectable through the set delta surface until the next
     * mutation epoch begins.
     */
    [[nodiscard]] SetView set_value() const;

    /**
     * Return the dictionary value surface for this time-series.
     *
     * The stored value is built with delta tracking, so the returned map view
     * retains stable-slot semantics needed by the time-series layer. Removed
     * key/value payloads remain inspectable through the map delta surface
     * until the next mutation epoch begins.
     */
    [[nodiscard]] MapView dict_value() const;

    /**
     * Return the window value surface for this time-series.
     */
    [[nodiscard]] CyclicBufferView window_value() const;

    /**
     * Return the list delta surface backed by the stored delta-tracking
     * value.
     *
     * This is a time-series-facing integration point over the new value
     * system. `TSValue` deliberately owns its `Value` with
     * `MutationTracking::Delta` so list outputs can ask the value layer for
     * the current updated/added slots instead of re-deriving that information
     * from a second storage structure.
     */
    [[nodiscard]] ListDeltaView list_delta_value() const;

    /**
     * Return the bundle delta surface backed by the stored delta-tracking
     * value.
     */
    [[nodiscard]] BundleDeltaView bundle_delta_value() const;

    /**
     * Return the set delta surface backed by the stored delta-tracking value.
     *
     * This is the key integration point for `TSS`: it exposes added/removed
     * elements together with stable-slot access to recently removed payloads
     * until the next mutation epoch begins.
     */
    [[nodiscard]] SetDeltaView set_delta_value() const;

    /**
     * Return the dictionary delta surface backed by the stored delta-tracking
     * value.
     *
     * This is the key integration point for `TSD`: it exposes added/removed/
     * updated slots together with stable-slot access to recently removed
     * key/value payloads until the next mutation epoch begins.
     */
    [[nodiscard]] MapDeltaView dict_delta_value() const;

    /**
     * Return the schema-resolved context needed to construct a TS view.
     */
    [[nodiscard]] ViewContext view_context() noexcept
    {
        return ViewContext{
            &schema(),
            &builder().value_builder().dispatch(),
            value_memory(),
            root_state()};
    }

private:
    /**
     * Return the conceptual root time-series state for this stored value.
     *
     * The current TS prototype still materialises the TS extension region as a
     * `TimeSeriesStateV`. `ViewContext` is the only TS-facing carrier that
     * should expose that conceptual root, so this helper remains private and
     * returns the raw state pointer directly.
     */
    [[nodiscard]] void *root_state() noexcept
    {
        return std::visit([](auto &state_value) -> void * { return &state_value; }, state_variant());
    }

    [[nodiscard]] const TSValueBuilder &builder() const noexcept { return *m_builder; }
    [[nodiscard]] void *storage_memory() noexcept { return m_storage; }
    [[nodiscard]] const void *storage_memory() const noexcept { return m_storage; }
    [[nodiscard]] void *value_memory() noexcept { return builder().value_memory(storage_memory()); }
    [[nodiscard]] const void *value_memory() const noexcept { return builder().value_memory(storage_memory()); }
    [[nodiscard]] void *ts_memory() noexcept { return builder().ts_memory(storage_memory()); }
    [[nodiscard]] const void *ts_memory() const noexcept { return builder().ts_memory(storage_memory()); }
    [[nodiscard]] TimeSeriesStateV &state_variant() noexcept { return *static_cast<TimeSeriesStateV *>(ts_memory()); }
    [[nodiscard]] const TimeSeriesStateV &state_variant() const noexcept { return *static_cast<const TimeSeriesStateV *>(ts_memory()); }

    void allocate_and_construct();
    void clear_storage() noexcept;

    const TSValueBuilder *m_builder{nullptr};
    /**
     * One combined allocation containing the value region followed by the TS
     * extension region.
     */
    void *                m_storage{nullptr};
    /**
     * Logical time-series schema describing the combined value and TS state.
     */
    std::reference_wrapper<const TSMeta> m_schema;
};

}  // namespace hgraph
