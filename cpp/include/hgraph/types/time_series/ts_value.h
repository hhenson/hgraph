#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_value_builder.h>
#include <hgraph/util/tagged_ptr.h>

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
 * - a time-series extension region described by `TSBuilderOps`
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
     * Construct an unbound time-series value placeholder.
     *
     * This is intended for delayed builder-driven construction, for example
     * when a node owns `TSValue` storage as a member and the node builder
     * later binds and constructs that storage in place.
     */
    TSValue() noexcept = default;

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
    enum class StorageOwnership : uint8_t
    {
        External,
        Owned = 1,
    };

    /**
     * Return the logical time-series schema satisfied by this storage.
     */
    [[nodiscard]] const TSMeta &schema() const noexcept { return builder().schema(); }

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
    [[nodiscard]] BufferView window_value() const;

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
    [[nodiscard]] TSViewContext view_context() noexcept
    {
        if (m_builder == nullptr || storage_memory() == nullptr) { return TSViewContext::none(); }
        return TSViewContext{
            &schema(),
            &builder().value_builder().dispatch(),
            &builder().ts_dispatch(),
            value_memory(),
            ts_root_state()};
    }

    /**
     * Return the binding handle for this logical TS position.
     *
     * Link-backed storage nodes use this to bind to the represented
     * time-series shape while keeping their own local link state as the owning
     * storage node.
     */
    [[nodiscard]] LinkedTSContext linked_context() noexcept
    {
        const TSViewContext context = view_context();
        if (!context.is_bound()) { return LinkedTSContext::none(); }
        return LinkedTSContext{
            context.schema,
            context.value_dispatch,
            context.ts_dispatch,
            context.value_data,
            context.ts_state,
        };
    }

    void attach_storage(void *storage) noexcept { m_storage.set_ptr(storage); }
    void detach_storage() noexcept { m_storage.set_ptr(nullptr); }
    void rebind_builder(const TSValueBuilder &builder, StorageOwnership storage_ownership) noexcept
    {
        m_builder = &builder;
        m_storage.set_tag(storage_ownership_tag(storage_ownership));
    }

    void reset_binding() noexcept
    {
        m_builder = nullptr;
        m_storage.clear();
    }

    /**
     * Return the conceptual root time-series state for this stored value.
     *
     * Derived endpoint/runtime helpers use this when they need to propagate
     * notifications into an owned derived representation such as an output
     * alternative.
     */
    [[nodiscard]] BaseState *ts_root_state() noexcept
    {
        return std::visit([](auto &state_value) -> BaseState * { return &state_value; }, state_variant());
    }

    [[nodiscard]] const BaseState *ts_root_state() const noexcept
    {
        return std::visit([](const auto &state_value) -> const BaseState * { return &state_value; }, state_variant());
    }

    [[nodiscard]] TimeSeriesStateV &state_variant() noexcept { return *static_cast<TimeSeriesStateV *>(ts_memory()); }
    [[nodiscard]] const TimeSeriesStateV &state_variant() const noexcept { return *static_cast<const TimeSeriesStateV *>(ts_memory()); }
    [[nodiscard]] TimeSeriesStateV &root_state_variant_ref() noexcept { return state_variant(); }
    [[nodiscard]] const TimeSeriesStateV &root_state_variant_ref() const noexcept { return state_variant(); }

  private:
    friend struct TSValueBuilder;
    friend struct TSInputBuilder;
    friend struct TSOutputBuilder;

    [[nodiscard]] const TSValueBuilder &builder() const noexcept { return *m_builder; }
    [[nodiscard]] void *storage_memory() noexcept { return m_storage.ptr(); }
    [[nodiscard]] const void *storage_memory() const noexcept { return m_storage.ptr(); }
    [[nodiscard]] void *value_memory() noexcept { return builder().value_memory(storage_memory()); }
    [[nodiscard]] const void *value_memory() const noexcept { return builder().value_memory(storage_memory()); }
    [[nodiscard]] void *ts_memory() noexcept { return builder().ts_memory(storage_memory()); }
    [[nodiscard]] const void *ts_memory() const noexcept { return builder().ts_memory(storage_memory()); }
    [[nodiscard]] bool owns_storage() const noexcept
    {
        return m_storage.has_tag(storage_ownership_tag(StorageOwnership::Owned));
    }

    using StoragePtr = erased_tagged_ptr<alignof(void *), 1>;
    [[nodiscard]] static constexpr typename StoragePtr::storage_type storage_ownership_tag(StorageOwnership ownership) noexcept
    {
        return static_cast<typename StoragePtr::storage_type>(ownership);
    }

    void allocate_and_construct();
    void clear_storage() noexcept;

    /**
     * The builder is the authoritative metadata handle for a bound `TSValue`.
     *
     * This cannot be recovered from schema alone because embedded `TSValue`
     * regions, such as those inside `TSInput`, may use plan-specialized
     * builders with different TS state layout and lifecycle behavior while
     * still representing the same logical schema.
     */
    const TSValueBuilder *m_builder{nullptr};
    /**
     * One combined allocation containing the value region followed by the TS
     * extension region. Bit 0 carries whether this handle owns destruction and
     * deallocation of that storage.
     */
    StoragePtr m_storage;
};

}  // namespace hgraph
