#include <hgraph/runtime/distributed_protocol.h>

#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/binary_codec.h>
#include <hgraph/types/value/value_view.h>

#include <fmt/format.h>

#include <cstring>
#include <stdexcept>
#include <unordered_set>

namespace hgraph::distributed
{
    namespace
    {
        /** A cycle time is fixed-width: it is present on every message. */
        void write_time(DateTime when, std::string &out)
        {
            const std::int64_t micros = when.time_since_epoch().count();
            char               bytes[sizeof(micros)];
            std::memcpy(bytes, &micros, sizeof(micros));
            out.append(bytes, sizeof(bytes));
        }

        [[nodiscard]] DateTime read_time(BinaryReader &reader)
        {
            std::int64_t micros = 0;
            std::memcpy(&micros, reader.take(sizeof(micros)), sizeof(micros));
            return DateTime{TimeDelta{micros}};
        }

        void write_text(std::string_view text, std::string &out)
        {
            write_varint(text.size(), out);
            out.append(text);
        }

        [[nodiscard]] std::string read_text(BinaryReader &reader)
        {
            const auto  size = static_cast<std::size_t>(read_varint(reader));
            const auto *raw  = reader.take(size);
            return std::string{reinterpret_cast<const char *>(raw), size};
        }

        void write_deltas(const BoundarySlots &slots, const std::vector<SlotDelta> &deltas,
                          std::string &out, SlotDirection direction)
        {
            write_varint(deltas.size(), out);
            std::unordered_set<std::size_t> seen;
            seen.reserve(deltas.size());
            for (const auto &entry : deltas)
            {
                // Validated on the way out as well as in: an out-of-range slot
                // written here would be indistinguishable from a corrupt stream
                // at the far end, and the sender is where the bug is.
                const auto *schema = slots.schema_at(entry.slot);
                if (!seen.insert(entry.slot).second)
                    throw std::runtime_error("distributed protocol: duplicate slot update");
                if (slots.direction_at(entry.slot) != direction)
                    throw std::runtime_error("distributed protocol: incorrect slot direction");
                write_varint(entry.slot, out);

                // Each payload carries its own length even though the codec is
                // self-delimiting given the schema. It bounds a corrupt decode
                // to one slot instead of letting it consume the rest of the
                // message, and it is what lets the exact-length check below
                // reject trailing bytes per value.
                std::string encoded;
                if (!entry.delta.has_value() || entry.delta.view().schema() != schema)
                {
                    throw std::logic_error(
                        fmt::format("distributed protocol: slot {} carries '{}' but the boundary "
                                    "declares '{}'",
                                    entry.slot, entry.delta.has_value() ? entry.delta.view().schema()->name() : "unset",
                                    schema->name()));
                }
                to_binary_string(entry.delta.view(), encoded);
                write_text(encoded, out);
            }
        }

        [[nodiscard]] std::vector<SlotDelta> read_deltas(const BoundarySlots &slots,
                                                         BinaryReader &reader, SlotDirection direction)
        {
            const auto             count = static_cast<std::size_t>(read_varint(reader));
            // Each entry needs at least a slot and a payload-length byte.
            // Bound allocation by bytes already received, not an unchecked count.
            if (count > reader.remaining() / 2)
                throw std::runtime_error("distributed protocol: truncated slot inventory");
            reader.consume_work(count);
            std::vector<SlotDelta> deltas;
            deltas.reserve(count);
            std::unordered_set<std::size_t> seen;
            seen.reserve(count);
            for (std::size_t i = 0; i < count; ++i)
            {
                const auto  slot    = static_cast<std::size_t>(read_varint(reader));
                const auto *schema  = slots.schema_at(slot);
                if (!seen.insert(slot).second)
                    throw std::runtime_error("distributed protocol: duplicate slot update");
                if (slots.direction_at(slot) != direction)
                    throw std::runtime_error("distributed protocol: incorrect slot direction");
                auto encoded = reader.subreader(static_cast<std::size_t>(read_varint(reader)));
                auto value = bind_binary_converter(schema).read(encoded);
                if (encoded.remaining() != 0)
                    throw std::runtime_error("distributed protocol: trailing bytes after slot value");
                deltas.push_back(SlotDelta{slot, std::move(value)});
            }
            return deltas;
        }

        [[nodiscard]] std::string_view finish(BinaryReader &reader, const char *what)
        {
            if (reader.remaining() != 0)
            {
                throw std::runtime_error(
                    fmt::format("distributed protocol: trailing bytes after a {}", what));
            }
            return {};
        }
    }  // namespace

    std::size_t BoundarySlots::add(std::string name, const ValueTypeMetaData *schema,
                                   SlotDirection direction)
    {
        if (schema == nullptr)
        {
            throw std::logic_error("distributed protocol: a boundary slot needs a schema");
        }
        if (index_of(name) != slots_.size())
        {
            throw std::logic_error(
                fmt::format("distributed protocol: duplicate boundary slot '{}'", name));
        }
        slots_.push_back(Slot{std::move(name), schema, direction});
        return slots_.size() - 1;
    }

    std::string_view BoundarySlots::name_at(std::size_t index) const
    {
        if (index >= slots_.size())
        {
            throw std::out_of_range(
                fmt::format("distributed protocol: slot {} is outside the boundary", index));
        }
        return slots_[index].name;
    }

    const ValueTypeMetaData *BoundarySlots::schema_at(std::size_t index) const
    {
        if (index >= slots_.size())
        {
            // An unknown slot is an error rather than a skip: silently dropping
            // a boundary value surfaces much later as a missing tick.
            throw std::out_of_range(
                fmt::format("distributed protocol: slot {} is outside the boundary", index));
        }
        return slots_[index].schema;
    }

    SlotDirection BoundarySlots::direction_at(std::size_t index) const
    {
        if (index >= slots_.size())
        {
            throw std::out_of_range(
                fmt::format("distributed protocol: slot {} is outside the boundary", index));
        }
        return slots_[index].direction;
    }

    std::size_t BoundarySlots::index_of(std::string_view name) const noexcept
    {
        for (std::size_t i = 0; i < slots_.size(); ++i)
        {
            if (slots_[i].name == name) { return i; }
        }
        return slots_.size();
    }

    std::string encode_request(const BoundarySlots &slots, const CycleRequest &request)
    {
        std::string out;
        write_time(request.evaluation_time, out);
        write_deltas(slots, request.staged, out, SlotDirection::Input);
        return out;
    }

    CycleRequest decode_request(const BoundarySlots &slots, std::string_view payload, BinaryDecodeLimits limits)
    {
        BinaryReader reader{payload, 0, limits};
        CycleRequest request;
        request.evaluation_time = read_time(reader);
        request.staged          = read_deltas(slots, reader, SlotDirection::Input);
        static_cast<void>(finish(reader, "request"));
        return request;
    }

    std::string encode_reply(const BoundarySlots &slots, const CycleReply &reply)
    {
        std::string out;
        write_time(reply.next_scheduled_time, out);
        write_deltas(slots, reply.collected, out, SlotDirection::Output);
        write_text(reply.error, out);
        return out;
    }

    CycleReply decode_reply(const BoundarySlots &slots, std::string_view payload, BinaryDecodeLimits limits)
    {
        BinaryReader reader{payload, 0, limits};
        CycleReply   reply;
        reply.next_scheduled_time = read_time(reader);
        reply.collected           = read_deltas(slots, reader, SlotDirection::Output);
        reply.error               = read_text(reader);
        static_cast<void>(finish(reader, "reply"));
        return reply;
    }

    std::string write_frame(std::string_view payload, std::size_t max_size)
    {
        if (payload.size() > max_size)
            throw std::runtime_error("distributed protocol: frame size limit exceeded");
        std::string out;
        write_varint(payload.size(), out);
        out.append(payload);
        return out;
    }

    bool read_frame(std::string_view buffer, std::string_view &payload, std::size_t &consumed,
                    std::size_t max_size)
    {
        // Only a short prefix is incomplete. Ten continuation bytes (or an
        // overflowing tenth byte) can never become valid with more input.
        std::uint64_t size = 0;
        std::size_t prefix = 0;
        for (; prefix < 10; ++prefix)
        {
            if (prefix == buffer.size()) return false;
            const auto byte = static_cast<unsigned char>(buffer[prefix]);
            if (prefix == 9 && (byte & 0xfeu) != 0)
                throw std::runtime_error("distributed protocol: frame varint overflow");
            size |= static_cast<std::uint64_t>(byte & 0x7fu) << (7 * prefix);
            if ((byte & 0x80u) == 0) { ++prefix; break; }
        }
        if (size > max_size)
            throw std::runtime_error("distributed protocol: frame size limit exceeded");
        if (buffer.size() - prefix < size) return false;
        payload = buffer.substr(prefix, static_cast<std::size_t>(size));
        consumed = prefix + static_cast<std::size_t>(size);
        return true;
    }

}  // namespace hgraph::distributed

// --- the worker's behaviour ------------------------------------------------
// Placed beside the protocol rather than in a worker binary: serving a cycle
// is what a worker IS, and keeping it here means it is exercised by the core
// test suite rather than only by whatever spawns a process.

#include <hgraph/runtime/distributed_child.h>

namespace hgraph::distributed
{
    CycleReply serve_cycle(const DistributedChildHost &host, const BoundarySlots &slots,
                           const CycleRequest &request)
    {
        CycleReply reply;
        try
        {
            for (const auto &staged : request.staged)
            {
                if (slots.direction_at(staged.slot) != SlotDirection::Input)
                {
                    throw std::logic_error(
                        fmt::format("distributed worker: slot {} is an output and cannot be staged",
                                    staged.slot));
                }
                host.stage(slots.name_at(staged.slot), staged.delta.view());
            }

            if (!host.step(request.evaluation_time))
            {
                // A root graph has no enclosing mesh to resolve a pause, so the
                // executor throws rather than returning false; reaching here
                // would mean that contract changed under us.
                throw std::logic_error("distributed worker: the child paused mid-cycle");
            }

            for (std::size_t slot = 0; slot < slots.size(); ++slot)
            {
                if (slots.direction_at(slot) != SlotDirection::Output) { continue; }
                Value collected = host.collect(slots.name_at(slot));
                if (!collected.has_value()) { continue; }   // no tick, so nothing to send
                reply.collected.push_back(SlotDelta{slot, std::move(collected)});
            }
            reply.next_scheduled_time = host.next_scheduled_time();
        }
        catch (const std::exception &error)
        {
            // Rendered here because the far side cannot catch it. Partial
            // output is dropped: a cycle either produced a result or failed.
            reply.collected.clear();
            reply.next_scheduled_time = MAX_DT;
            reply.error               = error.what();
        }
        return reply;
    }
}  // namespace hgraph::distributed
