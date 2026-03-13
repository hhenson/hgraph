#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_value.h>

namespace hgraph {

/**
 * Owning output-side time-series endpoint.
 *
 * `TSOutput` is intended to represent the producer-facing endpoint on a node.
 * It is the authoritative owner of the output-side `TSValue` payload and the
 * source of truth for leaf values published by that output, together with any
 * output-local runtime state exposed through the Python time-series output
 * API.
 *
 * `TSOutput` is intended to own the bulk of the time-series data. Leaf values
 * are stored here, and reference-oriented representations of the same logical
 * output are also owned here so they can be surfaced through the public API
 * without changing the underlying endpoint identity.
 *
 * Collection outputs are intended to hold their native structure directly. Any
 * peered input observing that structure is expected to do so through
 * `TargetLinkState`, rather than by taking ownership of duplicated output leaf
 * storage.
 */
struct HGRAPH_EXPORT TSOutput : TSValue {
    /**
     * Construct an output endpoint.
     *
     * Outputs are schema-bound. The supplied schema defines the published
     * time-series value shape owned by this endpoint.
     */
    explicit TSOutput(const TSMeta *schema) :
        TSValue(*schema)
    {}

    [[nodiscard]] TSOutputView view();

protected:
    /**
     * Return the registered alternative representations for this output.
     */
    [[nodiscard]] const std::unordered_map<const TSMeta *, TSValue> &alternatives() const noexcept { return m_alternatives; }

    /**
     * Locate an existing alternative representation for the supplied schema.
     */
    [[nodiscard]] TSValue *find_alternative(const TSMeta *schema) noexcept;

    /**
     * Locate an existing alternative representation for the supplied schema.
     */
    [[nodiscard]] const TSValue *find_alternative(const TSMeta *schema) const noexcept;

    /**
     * Create or return the alternative representation for the supplied schema.
     */
    [[nodiscard]] TSValue &ensure_alternative(const TSMeta *schema);

    /**
     * Remove any alternative representation for the supplied schema.
     */
    void remove_alternative(const TSMeta *schema) noexcept;

private:
    std::unordered_map<const TSMeta *, TSValue> m_alternatives;
};

}  // namespace hgraph
