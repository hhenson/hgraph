#ifndef HGRAPH_TYPES_TIME_SERIES_TS_DATA_CHECKPOINT_H
#define HGRAPH_TYPES_TIME_SERIES_TS_DATA_CHECKPOINT_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace hgraph
{
    class TSDataView;
    struct TSValueTypeMetaData;

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
        static constexpr std::uint32_t current_version = 1;
        std::uint32_t version{current_version};
        const TSValueTypeMetaData *schema{nullptr};
        DateTime last_modified_time{MIN_DT};
        Value payload{};
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
        bool (*eligible_impl)(const TSDataView &);
        TSCheckpointImage (*capture_impl)(const TSDataView &);
        void (*validate_impl)(const TSDataView &, const TSCheckpointImage &);
        /** Called only after complete image validation; must not publish ticks. */
        void (*restore_impl)(const TSDataView &, const TSCheckpointImage &);
    };

    /**
     * Checkpoint support is conservative and recursively representation-aware.
     * Built-in TS/SIGNAL, TSB, fixed/dynamic TSL, TSS and TSD support it. REF,
     * TSW, opaque Python storage and unsupported projections refuse it.
     */
    [[nodiscard]] HGRAPH_EXPORT bool ts_checkpoint_eligible(const TSDataView &source);
    [[nodiscard]] HGRAPH_EXPORT TSCheckpointImage capture_ts_checkpoint(const TSDataView &source);
    /** Validate schema, shape, payload and freshness without modifying the target. */
    HGRAPH_EXPORT void validate_ts_checkpoint(const TSDataView &target, const TSCheckpointImage &image);
    /** Quiet import into fresh endpoint storage. All validation precedes import. */
    HGRAPH_EXPORT void restore_ts_checkpoint(const TSDataView &target, const TSCheckpointImage &image);

    namespace ts_checkpoint_detail
    {
        /** Canonical refusing table for unsupported, alias and external strategies. */
        [[nodiscard]] HGRAPH_EXPORT const TSCheckpointOps &unsupported_checkpoint_ops() noexcept;
    }
}

#endif
