#ifndef HGRAPH_RUNTIME_DISTRIBUTED_PROTOCOL_H
#define HGRAPH_RUNTIME_DISTRIBUTED_PROTOCOL_H

// The messages between a dmap_ caller and one worker (RFC 0037).
//
// One request and one reply per engine cycle, carrying only what changed:
//
//     request  { evaluation_time, [ (slot, delta) ... ] }
//     reply    { next_scheduled_time, [ (slot, delta) ... ], error }
//
// That is the nested-graph contract -- evaluation time in,
// ``next_scheduled_time`` out -- with the deltas copied rather than shared.
//
// Slots travel as INDICES, not names. Both sides declare the same boundary in
// the same order because both built the same graph from the same wiring code,
// so a name on every message would be a per-cycle cost for information the
// receiver already has (RFC 0017, "Indexing by position, not by name"; the
// identity model is RFC 0022's, where a manifest validates agreement rather
// than transporting it).

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    struct ValueTypeMetaData;
}

namespace hgraph::distributed
{
    /** One boundary slot's value for one cycle. */
    struct HGRAPH_CLASS_EXPORT SlotDelta
    {
        std::size_t slot{0};
        Value       delta{};
    };

    /** One cycle's work for one worker. */
    struct HGRAPH_CLASS_EXPORT CycleRequest
    {
        DateTime               evaluation_time{MIN_ST};
        std::vector<SlotDelta> staged{};
    };

    /** One cycle's result from one worker. */
    struct HGRAPH_CLASS_EXPORT CycleReply
    {
        /** What the child wants next; ``MAX_DT`` when it wants nothing. */
        DateTime               next_scheduled_time{MAX_DT};
        std::vector<SlotDelta> collected{};
        /** Empty on success; otherwise the failure, already rendered. */
        std::string            error{};
    };

    /**
     * The ordered boundary of one child: what each slot is called and the
     * schema its deltas carry.
     *
     * Order is the contract. Both sides must declare the same slots in the
     * same order; the index is what travels.
     */
    class HGRAPH_CLASS_EXPORT BoundarySlots
    {
      public:
        /** Append one slot; returns its index. */
        std::size_t add(std::string name, const ValueTypeMetaData *schema);

        [[nodiscard]] std::size_t size() const noexcept { return slots_.size(); }
        [[nodiscard]] std::string_view name_at(std::size_t index) const;
        [[nodiscard]] const ValueTypeMetaData *schema_at(std::size_t index) const;
        /** The index of ``name``, or ``size()`` when it is not a slot here. */
        [[nodiscard]] std::size_t index_of(std::string_view name) const noexcept;

      private:
        std::vector<std::pair<std::string, const ValueTypeMetaData *>> slots_{};
    };

    [[nodiscard]] HGRAPH_EXPORT std::string encode_request(const BoundarySlots &slots,
                                                           const CycleRequest &request);
    [[nodiscard]] HGRAPH_EXPORT CycleRequest decode_request(const BoundarySlots &slots,
                                                            std::string_view payload);

    [[nodiscard]] HGRAPH_EXPORT std::string encode_reply(const BoundarySlots &slots,
                                                         const CycleReply &reply);
    [[nodiscard]] HGRAPH_EXPORT CycleReply decode_reply(const BoundarySlots &slots,
                                                        std::string_view payload);

    /**
     * Length-prefix one message for a byte stream.
     *
     * A stream has no message boundaries of its own. ``read_frame`` reports
     * how many bytes it consumed so a reader keeps a partial buffer rather
     * than assuming one read yields one whole message -- which it will not,
     * for any message worth distributing.
     */
    [[nodiscard]] HGRAPH_EXPORT std::string write_frame(std::string_view payload);

    /**
     * Extract one complete message from ``buffer``.
     *
     * Returns false when the buffer does not yet hold a whole message, leaving
     * the out-parameters untouched.
     */
    [[nodiscard]] HGRAPH_EXPORT bool read_frame(std::string_view buffer, std::string_view &payload,
                                                std::size_t &consumed);
}  // namespace hgraph::distributed

#endif  // HGRAPH_RUNTIME_DISTRIBUTED_PROTOCOL_H
