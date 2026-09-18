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
#include <hgraph/types/value/binary_codec.h>
#include <hgraph/util/date_time.h>

#include <ankerl/unordered_dense.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    struct ValueTypeMetaData;
}

namespace hgraph::distributed
{
    inline constexpr std::size_t DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;

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

    /** Which way a boundary slot carries values. */
    enum class SlotDirection : std::uint8_t
    {
        Input,   ///< staged by the caller, applied by the child
        Output,  ///< produced by the child, collected by the caller
    };

    /**
     * The ordered boundary of one child: what each slot is called, the schema
     * its deltas carry, and which way it goes.
     *
     * Order is the contract. Both sides must declare the same slots in the
     * same order; the index is what travels.
     *
     * Built at wiring time, and that is where each slot's converter is bound
     * (RFC 0040: ``Fast``, the profile for bytes that live for one cycle).
     * Binding locks the type system and allocates, so a message that bound per
     * slot put both on every cycle, in both directions; encoding and decoding
     * a cycle now take no lock.
     */
    class HGRAPH_CLASS_EXPORT BoundarySlots
    {
      public:
        /** Append one slot; returns its index. */
        std::size_t add(std::string name, const ValueTypeMetaData *schema, SlotDirection direction);

        [[nodiscard]] std::size_t size() const noexcept { return slots_.size(); }
        [[nodiscard]] std::string_view name_at(std::size_t index) const;
        [[nodiscard]] const ValueTypeMetaData *schema_at(std::size_t index) const;
        [[nodiscard]] SlotDirection direction_at(std::size_t index) const;
        /** The converter bound for the slot's schema when it was added. */
        [[nodiscard]] const BoundBinaryConverter &converter_at(std::size_t index) const;
        /** The index of ``name``, or ``size()`` when it is not a slot here. */
        [[nodiscard]] std::size_t index_of(std::string_view name) const noexcept;

      private:
        struct Slot
        {
            std::string              name;
            const ValueTypeMetaData *schema;
            SlotDirection            direction;
            BoundBinaryConverter     converter;
        };
        struct NameHash
        {
            using is_transparent = void;
            using is_avalanching = void;
            [[nodiscard]] std::uint64_t operator()(std::string_view text) const noexcept
            {
                return ankerl::unordered_dense::hash<std::string_view>{}(text);
            }
        };
        [[nodiscard]] const Slot &slot_at(std::size_t index) const;

        std::vector<Slot> slots_{};
        ankerl::unordered_dense::map<std::string, std::size_t, NameHash, std::equal_to<>> index_{};
    };

    [[nodiscard]] HGRAPH_EXPORT std::string encode_request(const BoundarySlots &slots,
                                                           const CycleRequest &request);
    [[nodiscard]] HGRAPH_EXPORT CycleRequest decode_request(const BoundarySlots &slots,
                                                            std::string_view payload, BinaryDecodeLimits limits = {});

    [[nodiscard]] HGRAPH_EXPORT std::string encode_reply(const BoundarySlots &slots,
                                                         const CycleReply &reply);
    [[nodiscard]] HGRAPH_EXPORT CycleReply decode_reply(const BoundarySlots &slots,
                                                        std::string_view payload, BinaryDecodeLimits limits = {});

    /**
     * Length-prefix one message for a byte stream.
     *
     * A stream has no message boundaries of its own. ``read_frame`` reports
     * how many bytes it consumed so a reader keeps a partial buffer rather
     * than assuming one read yields one whole message -- which it will not,
     * for any message worth distributing.
     */
    [[nodiscard]] HGRAPH_EXPORT std::string write_frame(std::string_view payload,
                                                        std::size_t max_size = DEFAULT_MAX_FRAME_SIZE);

    /**
     * Extract one complete message from ``buffer``.
     *
     * Returns false when the buffer does not yet hold a whole message, leaving
     * the out-parameters untouched. Malformed or oversized prefixes throw as
     * soon as their length is known, before buffering a payload.
     */
    [[nodiscard]] HGRAPH_EXPORT bool read_frame(std::string_view buffer, std::string_view &payload,
                                                std::size_t &consumed,
                                                std::size_t max_size = DEFAULT_MAX_FRAME_SIZE);
}  // namespace hgraph::distributed

#endif  // HGRAPH_RUNTIME_DISTRIBUTED_PROTOCOL_H
