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
    explicit TSValue(const TSMeta *schema) noexcept
        : m_value(*schema->value_type, MutationTracking::Delta)
        , m_schema(schema)
    {
    }

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
