#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/value/value.h>

#include <unordered_map>

namespace hgraph {

/**
 * Shared owning storage for a logical time-series value.
 *
 * `TSValue` is intended to hold the endpoint-local value payload and the root
 * time-series state tree that describes that payload.
 *
 * This is the shared storage contract used by both `TSInput` and `TSOutput`.
 * Endpoint-specific concerns such as input active-state tracking and output
 * alternative representations build on top of this base storage.
 *
 * The stored `value::Value` is expected to mirror the logical structure
 * described by `TSMeta`:
 * - `TSKind::TSValue`, `TSKind::TSS`, `TSKind::TSW`, `TSKind::REF`, and
 *   `TSKind::SIGNAL` store a single value compatible with
 *   `schema()->value_type`
 * - `TSKind::TSL` stores a value-level list whose items correspond positionally
 *   to `schema()->element_ts()`
 * - `TSKind::TSB` stores a value-level tuple/bundle whose fields correspond
 *   positionally to `schema()->fields()[i].ts_type`
 * - `TSKind::TSD` stores a value-level map whose values correspond to
 *   `schema()->element_ts()` and whose keys correspond to `schema()->key_type()`
 *
 * The state variant stored alongside the value is expected to describe the
 * same logical node as the root of that value shape. Child state held under
 * collection states is expected to follow the same positional or keyed layout
 * as the child values visible through the stored payload.
 */
struct HGRAPH_EXPORT TSValue {
    /**
     * Construct time-series value storage.
     *
     * Future constructors are expected to establish the time-series schema and
     * initialize the stored payload and root state tree.
     */
    TSValue() = default;

    /**
     * Return the logical time-series schema satisfied by this storage.
     */
    [[nodiscard]] const TSMeta *schema() const noexcept { return m_schema; }

    /**
     * Return the stored payload as a read-only value view.
     */
    [[nodiscard]] value::View value() const { return m_value.view(); }

    /**
     * Return the stored payload as a mutable value view.
     */
    [[nodiscard]] value::ValueView value_mut() { return m_value.view(); }

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

protected:
    /**
     * Owning value payload whose shape is governed by `m_schema`.
     */
    value::Value     m_value;
    /**
     * Root time-series state associated to `m_value`.
     */
    TimeSeriesStateV m_state;
    /**
     * Logical time-series schema describing `m_value` and `m_state`.
     */
    const TSMeta *   m_schema{nullptr};
};

}  // namespace hgraph
