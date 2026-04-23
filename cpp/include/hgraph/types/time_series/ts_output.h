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
    [[nodiscard]] TimeSeriesStateV &root_state_variant() noexcept { return root_state_variant_ref(); }
    [[nodiscard]] const TimeSeriesStateV &root_state_variant() const noexcept { return root_state_variant_ref(); }

    [[nodiscard]] TSOutputView view(engine_time_t evaluation_time = MIN_DT);

    /**
     * Return a bindable view matching the requested schema, creating an
     * output-owned alternative representation when required.
     */
    [[nodiscard]] TSOutputView bindable_view(const TSOutputView &source, const TSMeta *schema);

protected:
private:
    friend struct TSOutputBuilder;
    friend struct TSOutputView;
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
    struct DetachedAlternativeKey
    {
        BaseState *source_state{nullptr};
        const TSMeta *source_schema{nullptr};
        const TSMeta *target_schema{nullptr};

        [[nodiscard]] bool operator==(const DetachedAlternativeKey &other) const noexcept
        {
            return source_state == other.source_state && source_schema == other.source_schema && target_schema == other.target_schema;
        }
    };
    struct DetachedAlternativeKeyHash
    {
        [[nodiscard]] size_t operator()(const DetachedAlternativeKey &key) const noexcept
        {
            const auto hash_ptr = [](const void *value) noexcept { return std::hash<const void *>{}(value); };
            size_t seed = hash_ptr(key.source_state);
            seed ^= hash_ptr(key.source_schema) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= hash_ptr(key.target_schema) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            return seed;
        }
    };
    using DetachedAlternativeMap = std::unordered_map<DetachedAlternativeKey, AlternativePtr, DetachedAlternativeKeyHash>;

    [[nodiscard]] const TSOutputBuilder &builder() const noexcept { return *m_builder; }
    void clear_storage() noexcept;
    void clear_ref_target_subscriptions() noexcept;
    void sync_ref_target_subscriptions(engine_time_t evaluation_time);

    const TSOutputBuilder *m_builder{nullptr};
    AlternativeMap m_alternatives;
    DetachedAlternativeMap m_detached_alternatives;
    std::vector<RefTargetSubscription> m_ref_target_subscriptions;
};

/**
 * Publish a modification on an output view and keep any direct dynamic dict
 * children synchronized with the same engine tick.
 */
HGRAPH_EXPORT void mark_output_view_modified(const TSOutputView &view, engine_time_t evaluation_time);
HGRAPH_EXPORT void prepare_output_link(const TSOutputView &target);
[[nodiscard]] HGRAPH_EXPORT bool bind_output_link(const TSOutputView &target, const TSOutputView &source);
HGRAPH_EXPORT void clear_output_link(const TSOutputView &target);

}  // namespace hgraph
