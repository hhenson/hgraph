#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_output_builder.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_value.h>

#include <memory>
#include <vector>
#include <unordered_map>

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
 * peered input observing that structure is expected to do so through link-
 * backed TS state, rather than by taking ownership of duplicated output-side
 * storage.
 */
struct HGRAPH_EXPORT TSOutput : TSValue {
    /**
     * Construct an unbound output placeholder.
     *
     * This is intended for delayed builder-driven construction.
     */
    TSOutput() noexcept;
    explicit TSOutput(const TSOutputBuilder &builder);
    TSOutput(const TSOutput &other);
    TSOutput(TSOutput &&other) noexcept;
    TSOutput &operator=(const TSOutput &other);
    TSOutput &operator=(TSOutput &&other) noexcept;
    ~TSOutput();

    /**
     * Return the binding handle representing this output's current root view.
     */
    [[nodiscard]] LinkedTSContext linked_context() noexcept { return TSValue::linked_context(); }

    [[nodiscard]] TSOutputView view(engine_time_t evaluation_time = MIN_DT);

    /**
     * Return a bindable view matching the requested schema, creating an
     * output-owned alternative representation when required.
     */
    [[nodiscard]] TSOutputView bindable_view(const TSOutputView &source, const TSMeta *schema);

protected:
private:
    friend struct TSOutputBuilder;
    friend void mark_output_view_modified(const TSOutputView &view, engine_time_t evaluation_time);

    struct AlternativeOutput;
    struct RefTargetSubscription
    {
        BaseState *state{nullptr};
        std::unique_ptr<Notifiable> notifier;
    };
    struct AlternativeOutputDeleter
    {
        void operator()(AlternativeOutput *value) const noexcept;
    };

    using AlternativePtr = std::unique_ptr<AlternativeOutput, AlternativeOutputDeleter>;
    using AlternativeMap = std::unordered_map<const TSMeta *, AlternativePtr>;

    [[nodiscard]] const TSOutputBuilder &builder() const noexcept { return *m_builder; }
    void clear_storage() noexcept;
    void clear_ref_target_subscriptions() noexcept;
    void sync_ref_target_subscriptions(engine_time_t evaluation_time);

    const TSOutputBuilder *m_builder{nullptr};
    AlternativeMap m_alternatives;
    std::vector<RefTargetSubscription> m_ref_target_subscriptions;
};

/**
 * Publish a modification on an output view and keep any direct dynamic dict
 * children synchronized with the same engine tick.
 */
HGRAPH_EXPORT void mark_output_view_modified(const TSOutputView &view, engine_time_t evaluation_time);

}  // namespace hgraph
