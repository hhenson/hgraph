#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_value.h>

namespace hgraph {

/**
 * Owning input-side time-series endpoint.
 *
 * `TSInput` is intended to represent the consumer-facing endpoint on a node.
 * It owns the input-local runtime state needed for binding, activation, and
 * input-specific navigation as exposed through the Python time-series input
 * API.
 *
 * In addition to the inherited `TSValue` payload, `TSInput` owns a parallel
 * active-state value. This active payload is not the published time-series
 * value; it is a control structure used to answer `TSInputView::active()` and
 * to support `make_active()` / `make_passive()` at a specific input path.
 *
 * The active-state payload is intended to mirror the navigable structure of
 * the input:
 * - a leaf input position stores a local `bool`
 * - a collection input position stores its own local `bool` together with
 *   child active payloads laid out in child order
 *
 * The local active flag for a collection is independent from the active flags
 * of its children. This preserves the Python API behavior where a parent input
 * may be passive while one of its descendants is active. The collection-local
 * flag is therefore expected to be addressable directly from the collection's
 * active-state view, without inferring activation from descendant state.
 *
 * Native input-owned collection storage is intended to be used only for
 * non-peered `TSL` and `TSB` shapes. In that case the input owns the
 * collection structure and the child state directly.
 *
 * When an input is peered to an output, child positions are intended to be
 * represented through `TargetLinkState` rather than by duplicating leaf value
 * storage inside the input. The output-side endpoint remains the source of
 * truth for peered leaf values, while the input tracks binding and activation
 * state for its local view of that structure.
 */
struct HGRAPH_EXPORT TSInput : TSValue {
    /**
     * Construct an input endpoint.
     *
     * Inputs are schema-bound. The supplied schema defines both the published
     * time-series value shape and the parallel activation-control shape held
     * by this endpoint.
     */
    explicit TSInput(const TSMeta *schema) :
        TSValue(schema), m_active_state(active_schema_from(schema))
    {}

protected:
    /**
     * Return the hierarchical active-state payload as a read-only value view.
     *
     * This view is intended to expose the parallel activation schema
     * associated to the input tree, not the published time-series value.
     */
    [[nodiscard]] value::View active_state() const { return m_active_state.view(); }

    /**
     * Return the hierarchical active-state payload as a mutable value view.
     *
     * This is intended for view construction and activation control against
     * the input-local parallel active-state schema.
     */
    [[nodiscard]] value::ValueView active_state_mut() { return m_active_state.view(); }

private:
    /**
     * Map a time-series schema to the parallel active-state value schema.
     *
     * The returned value schema is intended to preserve the same navigable
     * shape as the input while replacing each input position with the active
     * control payload needed for that position.
     */
    [[nodiscard]] static const value::TypeMeta *active_schema_from(const TSMeta *schema);

    /**
     * Parallel activation payload aligned to the logical input structure.
     */
    value::Value m_active_state;
};

}  // namespace hgraph
