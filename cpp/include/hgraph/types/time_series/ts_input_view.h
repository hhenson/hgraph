#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/active_trie.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph {

struct TSOutputView;

/**
 * Input-specialized instantiation of the generic time-series view surface.
 *
 * `TSInputView` is intended to expose input-only behavior, especially binding
 * and activation control, while reusing the shared time-series navigation
 * contract.
 *
 * Activation is path-local by design. A parent input may remain passive while
 * one of its descendants is active. The concrete activation and binding
 * behavior lives behind runtime ops carried on the view context, so this type
 * remains a thin endpoint-specific facade over the shared TS navigation model.
 */
struct HGRAPH_EXPORT TSInputView : TSView<TSInputView> {
    using TSView<TSInputView>::TSView;

    /**
     * Construct a navigable input view over TS storage.
     *
     * This is the wiring-time/runtime navigation surface used for collection
     * access and binding.
     */
    ~TSInputView() = default;

    /**
     * Bind this collection-selected input slot to an output.
     *
     * This is intended to be called on a child view reached through `TSL` or
     * `TSB` navigation, where the current view identifies a prebuilt
     * target-link terminal from the input construction plan.
     */
    void bind_output(const TSOutputView &output);

    /**
     * Mark the input view as active.
     *
     * This is intended to enable active observation for the represented input
     * position so upstream notifications reach the owning input endpoint.
     * Activity is local to this path and does not imply parent activation.
     */
    void make_active();

    /**
     * Mark the input view as passive.
     *
     * This is intended to disable active observation for the represented input
     * position so upstream notifications are no longer requested.
     */
    void make_passive();

    /**
     * Return whether the input view is currently active.
     *
     * This is intended to report whether the represented input position is
     * currently participating in active observation.
     */
    [[nodiscard]] bool active() const;
};

}  // namespace hgraph
