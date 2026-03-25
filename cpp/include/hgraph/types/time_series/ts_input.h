#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_builder.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/value/value.h>

namespace hgraph {

/**
 * Owning input-side time-series endpoint.
 *
 * `TSInput` is intended to represent the consumer-facing endpoint on a node.
 * It owns the input-local runtime state needed for binding, activation, and
 * input-specific navigation as exposed through the Python time-series input
 * API.
 *
 * `TSInput` is built by a schema- and plan-bound `TSInputBuilder`. The
 * builder owns the combined layout for:
 * - a `TSValue` region representing the published TS payload and state
 * - a parallel active-state `Value` region used only for input activation
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
 * represented through link-backed TS state rather than by duplicating the
 * linked output-side storage inside the input. The output-side endpoint
 * remains the source of truth for those bound positions, while the input
 * tracks binding and activation state for its local view of that structure.
 */
struct HGRAPH_EXPORT TSInput : TSValue {
    /**
     * Construct an input endpoint.
     *
     * Inputs are builder-bound. The supplied builder owns the composite
     * storage layout together with the plan-specialized TS state construction
     * logic for this input shape.
     */
    explicit TSInput(const TSInputBuilder &builder);
    TSInput(const TSInput &other);
    TSInput(TSInput &&other) noexcept;
    TSInput &operator=(const TSInput &other);
    TSInput &operator=(TSInput &&other) noexcept;
    ~TSInput();

    /**
     * Return an input view rooted at this endpoint.
     *
     * Binding is expected to happen through collection navigation on this
     * view, matching the Python wiring model where `__getitem__` selects the
     * slot to bind.
     */
    [[nodiscard]] TSInputView view() noexcept;

protected:
    /**
     * Return the hierarchical active-state payload as a read-only value view.
     *
     * This view is intended to expose the parallel activation schema
     * associated to the input tree, not the published time-series value.
     */
    [[nodiscard]] View active_state() const;

    /**
     * Return the hierarchical active-state payload as a mutable value view.
     *
     * This is intended for view construction and activation control against
     * the input-local parallel active-state schema.
     */
    [[nodiscard]] View active_state();

private:
    [[nodiscard]] const TSInputBuilder &builder() const noexcept { return *m_builder; }
    [[nodiscard]] void *storage_memory() noexcept { return m_storage; }
    [[nodiscard]] const void *storage_memory() const noexcept { return m_storage; }
    [[nodiscard]] void *active_memory() noexcept { return builder().active_memory(storage_memory()); }
    [[nodiscard]] const void *active_memory() const noexcept { return builder().active_memory(storage_memory()); }

    void allocate_and_construct();
    void clear_storage() noexcept;

    const TSInputBuilder *m_builder{nullptr};
    void *m_storage{nullptr};
};

}  // namespace hgraph
