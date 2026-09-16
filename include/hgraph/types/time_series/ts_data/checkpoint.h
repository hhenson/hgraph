#ifndef HGRAPH_TYPES_TIME_SERIES_TS_DATA_CHECKPOINT_H
#define HGRAPH_TYPES_TIME_SERIES_TS_DATA_CHECKPOINT_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

namespace hgraph
{
    class TSDataView;
    struct TSValueTypeMetaData;
    struct TimeSeriesReference;

    /** Recreate a synthetic binding, then select a child of that binding. */
    struct TSCheckpointBindingStep
    {
        const TSValueTypeMetaData *requested_schema{nullptr};
        std::vector<std::size_t> path{};
        bool operator==(const TSCheckpointBindingStep &) const = default;
    };

    /** Component-relative endpoint identity; never contains runtime pointers.
     * graph_path consists of owning-node/child-slot pairs. Endpoint roles are
     * 0 output, 1 error, 2 recordable state, 3 ingress, 4 custom endpoint.
     * Structural paths are field/list/key-slot ordinals, including the key-set
     * path sentinel. Membership and endpoint storage exist before resolution.
     */
    struct TSCheckpointLocator
    {
        std::vector<std::size_t> graph_path{};
        std::size_t node{0};
        std::uint32_t endpoint{0};
        std::size_t custom_endpoint{0};
        std::vector<std::size_t> endpoint_path{};
        std::vector<TSCheckpointBindingStep> bindings{};
        bool operator==(const TSCheckpointLocator &) const = default;
    };

    enum class TSReferenceCheckpointKind : std::uint8_t { Empty = 0, Peered = 1, NonPeered = 2 };

    /** Owned reference value. Declared target schema can differ from the
     * located endpoint schema (for an explicitly adapted reference).
     */
    struct TSReferenceCheckpointImage
    {
        TSReferenceCheckpointKind kind{TSReferenceCheckpointKind::Empty};
        const TSValueTypeMetaData *target_schema{nullptr};
        std::optional<TSCheckpointLocator> target{};
        std::vector<TSReferenceCheckpointImage> items{};
        bool operator==(const TSReferenceCheckpointImage &) const = default;
    };

    /** Explicit cold-path ownership context. Restore queues reference fixups;
     * it must not publish or resolve them before all endpoints are restored.
     */
    struct TSCheckpointContext
    {
        std::function<TSReferenceCheckpointImage(const TimeSeriesReference &)> capture_reference{};
        std::function<void(const TSDataView &, const TSReferenceCheckpointImage &)> restore_reference{};
    };

    /** Validate data-only reference shape; ownership/resolution is context-owned. */
    HGRAPH_EXPORT void validate_ts_reference_checkpoint(const TSReferenceCheckpointImage &image);
    [[nodiscard]] HGRAPH_EXPORT bool ts_checkpoint_schema_contains_reference(const TSValueTypeMetaData *schema);


    /**
     * Owned semantic endpoint image at a completed evaluation cycle.
     *
     * No observer, borrowed view, parent pointer, or live runtime allocation is
     * retained. Atomic values use payload; fixed structures use ordered children;
     * keyed structures use keys/slots and, for dictionaries, matching children.
     * Invalid endpoints and partially valid children retain their own timestamps.
     * Transient deltas are deliberately absent: resume starts AFTER the cut.
     * Removed collection entries are normalized to erased; free_slots retains
     * the resulting allocator order so future inserted keys keep stable slots.
     *
     * Durable encoders serialize schema identity (not this process-local pointer),
     * every timestamp, the value payloads and the recursive structural metadata.
     * Decoders resolve schemas against the rebuilt graph before validation.
     */
    struct TSCheckpointImage
    {
        static constexpr std::uint32_t current_version = 3;
        std::uint32_t version{current_version};
        const TSValueTypeMetaData *schema{nullptr};
        DateTime last_modified_time{MIN_DT};
        Value payload{};
        std::optional<TSReferenceCheckpointImage> reference{};
        /** TSW only: one chronological time per live value in the dynamic-list
         * payload. No capacity padding, per-sample schema or recursive image.
         * Retained samples also survive endpoint invalidation; last_modified_time
         * independently retains endpoint validity. Expiry remains push-driven.
         */
        std::vector<DateTime> window_times{};
        std::vector<TSCheckpointImage> children{};
        std::vector<Value> keys{};
        std::vector<std::size_t> slots{};
        std::vector<std::size_t> free_slots{};
        std::size_t slot_capacity{0};
        DateTime key_set_last_modified_time{MIN_DT};
        std::vector<bool> published{};
    };

    /** Representation-selected passive contract; every TSData table has one. */
    struct TSCheckpointOps
    {
        bool (*eligible_impl)(const TSDataView &, const TSCheckpointContext *);
        TSCheckpointImage (*capture_impl)(const TSDataView &, const TSCheckpointContext *);
        void (*validate_impl)(const TSDataView &, const TSCheckpointImage &, const TSCheckpointContext *);
        /** Called only after complete image validation; must not publish ticks. */
        void (*restore_impl)(const TSDataView &, const TSCheckpointImage &, const TSCheckpointContext *);
    };

    /**
     * Checkpoint support is conservative and recursively representation-aware.
     * Built-in TS/SIGNAL, TSB, fixed/dynamic TSL, TSS, TSD and TSW support it.
     * REF requires an explicit component context. Opaque Python storage and
     * unsupported projections refuse it.
     */
    [[nodiscard]] HGRAPH_EXPORT bool ts_checkpoint_eligible(const TSDataView &source, const TSCheckpointContext *context = nullptr);
    [[nodiscard]] HGRAPH_EXPORT TSCheckpointImage capture_ts_checkpoint(const TSDataView &source, const TSCheckpointContext *context = nullptr);
    /** Validate schema, shape, payload and freshness without modifying the target. */
    HGRAPH_EXPORT void validate_ts_checkpoint(const TSDataView &target, const TSCheckpointImage &image,
                                               const TSCheckpointContext *context = nullptr);
    /** Quiet import into fresh endpoint storage. All validation precedes import. */
    HGRAPH_EXPORT void restore_ts_checkpoint(const TSDataView &target, const TSCheckpointImage &image,
                                               const TSCheckpointContext *context = nullptr);

    namespace ts_checkpoint_detail
    {
        /** Canonical refusing table for unsupported, alias and external strategies. */
        [[nodiscard]] HGRAPH_EXPORT const TSCheckpointOps &unsupported_checkpoint_ops() noexcept;
    }
}

#endif
