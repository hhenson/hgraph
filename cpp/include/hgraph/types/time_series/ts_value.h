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
    value::Value     m_value;
    TimeSeriesStateV m_state;
    const TSMeta *   m_schema{nullptr};
};

}  // namespace hgraph
