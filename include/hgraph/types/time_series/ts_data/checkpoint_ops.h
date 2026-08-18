#ifndef HGRAPH_TS_DATA_CHECKPOINT_OPS_H
#define HGRAPH_TS_DATA_CHECKPOINT_OPS_H

/**
 * @file checkpoint_ops.h
 * ``TSCheckpointOps`` — exact endpoint capture and quiet import (RFC 0023).
 *
 * A checkpoint image is the OWNED, EXACT semantic state of one owned
 * endpoint at a completed cut: current values, validity, original per-node
 * and per-child modification times, keyed-slot identity (capacity, live and
 * pending-erase planes, keys, free-list ordering), dynamic shape, and window
 * entries with their original timestamps.  Per-cycle delta planes, observer
 * lists, parent links, and cached Python wrappers are NOT part of the image
 * — they are per-cycle or process-local and are rebuilt by the restore
 * lifecycle.
 *
 * Stage 1 (RFC 0023) is an in-memory checkpoint: the image holds owned
 * ``Value``s and times, not encoded bytes.  Byte encoding belongs to the
 * durable serialisation strategy (RFC 0025: extension-owned).
 *
 * ``import`` is a QUIET write into unstarted storage: no output tick is
 * published, no observer or parent is notified, no consumer is scheduled,
 * and original modification times are retained.  The
 * ``TSCheckpointRestoreGuard`` parameter makes "only under the restore
 * lifecycle" a type-level fact.
 *
 * Like ``TSCurrentStateOps``, this is a separately selected, non-null,
 * cold-path erased policy hanging off ``TSDataOps``: semantic consumers
 * dispatch through it rather than recovering a representation from
 * ``kind``, and representations recursively invoke their children's
 * tables.  It adds no evaluation-time lookup.
 */

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace hgraph
{
    struct TSCheckpointImage;

    /** One constructed slot of a keyed kind (TSS/TSD), pending-erase included. */
    struct HGRAPH_EXPORT TSCheckpointSlotImage
    {
        std::size_t slot_id{0};
        /** False = constructed but pending erase. */
        bool live{true};
        Value key{};
        /** TSD only: the durable has-ever-published plane (drives removal events). */
        bool value_published{false};
        /** TSD only: the slot's child endpoint image. */
        std::unique_ptr<TSCheckpointImage> child{};
    };

    /** One retained window element with its ORIGINAL evaluation timestamp. */
    struct HGRAPH_EXPORT TSCheckpointWindowEntry
    {
        Value value{};
        DateTime time{MIN_DT};
    };

    /**
     * The owned exact image of one TS node.  Which members are populated
     * depends on ``kind``; unused members stay empty.  ``schema`` is the
     * interned runtime schema — valid for the in-memory strategy within one
     * process; a durable strategy replaces it with a canonical descriptor.
     */
    struct HGRAPH_EXPORT TSCheckpointImage
    {
        TSTypeKind kind{TSTypeKind::TS};
        const TSValueTypeMetaData *schema{nullptr};
        /** ``MIN_DT`` = never ticked (invalid). */
        DateTime modified_time{MIN_DT};

        /** Atomic kinds (TS / SIGNAL / REF): the current value; empty when invalid. */
        Value value{};

        /** TSB fields / fixed-TSL indices / dynamic-TSL elements, in order.
            For dynamic TSL, ``children.size()`` IS the durable shape. */
        std::vector<TSCheckpointImage> children{};

        // ----- keyed kinds (TSS / TSD) -----
        std::size_t slot_capacity{0};
        /** Constructed slots in ascending ``slot_id`` order. */
        std::vector<TSCheckpointSlotImage> slots{};
        /** The free-list in its EXACT vector order (erase history; determines
            every future key→slot assignment). */
        std::vector<std::size_t> free_slots{};
        /** Pending-erase slot ids in their exact order. */
        std::vector<std::size_t> pending_erase_slots{};
        /** TSD only: the key-set's own independent modification time. */
        DateTime key_set_modified_time{MIN_DT};

        // ----- window kinds (TSW) -----
        /** Retained entries oldest → newest with original timestamps. */
        std::vector<TSCheckpointWindowEntry> window{};
        /** hgraph's removed-value surface: last evicted element (may be empty). */
        Value evicted{};
        /** Eviction time, or the clear time when ``evicted`` is empty. */
        DateTime evicted_time{MIN_DT};
    };

    /** Path-addressed reason a validation or capture refused. */
    struct HGRAPH_EXPORT TSCheckpointDiagnostics
    {
        std::string path{};
        std::string reason{};
    };

    /**
     * Token proving the graph-wide restore lifecycle is active.  Quiet
     * import is only reachable through one of these; the executor's restore
     * operation owns creation (tests use ``begin`` directly).
     */
    class HGRAPH_EXPORT TSCheckpointRestoreGuard
    {
      public:
        [[nodiscard]] static TSCheckpointRestoreGuard begin() noexcept
        {
            return TSCheckpointRestoreGuard{};
        }

      private:
        TSCheckpointRestoreGuard() = default;
    };

    /**
     * The checkpoint operation table.  ``context`` is the owning
     * representation's interned ops context (the same pointer as
     * ``TSDataOps::context``); ``memory`` is the node's storage.
     */
    struct HGRAPH_EXPORT TSCheckpointOps
    {
        /** False = this representation refuses checkpointing (conservative,
            path-addressed refusal at eligibility time). */
        bool supported{false};

        /** Capture the owned exact image of this node (recursing into
            children through their own tables). */
        void (*capture_impl)(const void *context, const void *memory,
                             TSCheckpointImage &out) = nullptr;

        /** Validate a decoded image against this live binding without
            writing; false fills ``why``. */
        bool (*validate_impl)(const void *context, const TSCheckpointImage &image,
                              TSCheckpointDiagnostics &why) = nullptr;

        /** Quietly import ``image`` into UNSTARTED storage: exact state,
            original times, no publication of any kind. */
        void (*import_impl)(const void *context, void *memory,
                            const TSCheckpointImage &image,
                            const TSCheckpointRestoreGuard &guard) = nullptr;
    };

    namespace ts_checkpoint_detail
    {
        /** Canonical refusing table: ``supported == false``; every op throws
            naming the operation. */
        [[nodiscard]] HGRAPH_EXPORT const TSCheckpointOps &unsupported_checkpoint_ops() noexcept;
    }  // namespace ts_checkpoint_detail

    class TSDataView;

    /** Whether this endpoint's representation supports checkpointing. */
    [[nodiscard]] HGRAPH_EXPORT bool checkpoint_supported(const TSDataView &data);

    /** Capture the owned exact image of a live endpoint (schema stamped). */
    [[nodiscard]] HGRAPH_EXPORT TSCheckpointImage capture_checkpoint(const TSDataView &data);

    /** Validate a decoded image against a live binding without writing. */
    [[nodiscard]] HGRAPH_EXPORT bool validate_checkpoint(const TSDataView &data,
                                                         const TSCheckpointImage &image,
                                                         TSCheckpointDiagnostics &why);

    /** Quietly import an image into unstarted storage. */
    HGRAPH_EXPORT void import_checkpoint(const TSDataView &data,
                                         const TSCheckpointImage &image,
                                         const TSCheckpointRestoreGuard &guard);
}  // namespace hgraph

#endif  // HGRAPH_TS_DATA_CHECKPOINT_OPS_H
