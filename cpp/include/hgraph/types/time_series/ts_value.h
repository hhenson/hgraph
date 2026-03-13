#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/value/value.h>

namespace hgraph {

/**
 * Shared owning storage for a logical time-series value.
 *
 * `TSValue` is intended to hold the endpoint-local value together with the
 * root time-series state tree that describes modification and subscription
 * behavior for that value.
 *
 * This is the shared storage contract used by both `TSInput` and `TSOutput`.
 * Endpoint-specific concerns such as input active-state tracking and output
 * alternative representations build on top of this base storage.
 *
 * The stored `Value` is schema-bound to `TSMeta::value_type`. Time-series
 * specific owned-versus-linked behavior is expected to be expressed within the
 * `Value` / `View` layer through the concrete state implementations selected
 * by that value schema, rather than by adding a second top-level value-state
 * wrapper here.
 *
 * The time-series state stored alongside that value is responsible for
 * modification, subscription, rebinding, and other time-series-specific
 * runtime behavior.
 */
struct HGRAPH_EXPORT TSValue {
    /**
     * Construct time-series value storage.
     *
     * Time-series value storage is schema-bound. The supplied time-series
     * schema defines both the time-series behavior and the underlying value
     * schema used by the owned `Value`.
     */
    explicit TSValue(const TSMeta *schema);

protected:
    /**
     * Return the logical time-series schema satisfied by this storage.
     */
    [[nodiscard]] const TSMeta *schema() const noexcept { return m_schema; }

    /**
     * Return the stored endpoint value as a read-only erased view.
     */
    [[nodiscard]] View value() const noexcept { return m_value.view(); }

    /**
     * Return the stored endpoint value as a mutable erased view.
     */
    [[nodiscard]] View value() noexcept { return m_value.view(); }

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
     * Return the root time-series state for this stored value.
     */
    [[nodiscard]] TimeSeriesStatePtr state_ptr() noexcept
    {
        return std::visit(
            [](auto &state_value) -> TimeSeriesStatePtr {
                return TimeSeriesStatePtr{&state_value};
            },
            m_state);
    }

private:
    [[nodiscard]] static TimeSeriesStateV make_root_state(const TSMeta *schema);

    Value               m_value;
    /**
     * Root time-series state associated to `m_value`.
     */
    TimeSeriesStateV      m_state;
    /**
     * Logical time-series schema describing `m_value` and `m_state`.
     */
    const TSMeta *        m_schema{nullptr};
};

}  // namespace hgraph
