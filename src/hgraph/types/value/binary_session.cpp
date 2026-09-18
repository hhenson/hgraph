#include <hgraph/types/value/binary_session.h>

#include <hgraph/types/metadata/schema_table.h>

#include <fmt/format.h>

#include <cstring>
#include <stdexcept>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::size_t frame_header_bytes = 6;

        [[nodiscard]] BinaryProfile checked_profile(std::uint8_t byte)
        {
            if (byte > static_cast<std::uint8_t>(BinaryProfile::Fast))
            {
                throw std::runtime_error(fmt::format("binary codec: frame names unknown profile {}", byte));
            }
            return static_cast<BinaryProfile>(byte);
        }

        [[nodiscard]] std::string_view profile_name(BinaryProfile profile) noexcept
        {
            return profile == BinaryProfile::Fast ? "Fast" : "Compact";
        }
    }  // namespace

    // --- encode -------------------------------------------------------------

    struct BinaryEncodeSession::Impl
    {
        BinaryProfile profile{BinaryProfile::Compact};
        SchemaTableWriter schemas{};
        // One converter per value schema in the table, bound on first use.
        std::vector<BoundBinaryConverter> converters{};
    };

    BinaryEncodeSession::BinaryEncodeSession(BinaryProfile profile) : impl_(std::make_unique<Impl>())
    {
        impl_->profile = profile;
    }

    BinaryEncodeSession::~BinaryEncodeSession() = default;

    BinaryProfile BinaryEncodeSession::profile() const noexcept { return impl_->profile; }

    SchemaTableWriter &BinaryEncodeSession::schemas() noexcept { return impl_->schemas; }

    std::size_t BinaryEncodeSession::value_ref(const ValueTypeMetaData *schema)
    {
        if (schema == nullptr) { throw std::logic_error("binary codec: null schema"); }
        return impl_->schemas.value_ref(schema);
    }

    const BoundBinaryConverter &BinaryEncodeSession::converter_at(std::size_t index)
    {
        // Adding a schema adds what it is built from first, so the table can
        // have grown by more than one entry since the last call.
        auto &converters = impl_->converters;
        while (converters.size() < impl_->schemas.value_count())
        {
            converters.push_back(bind_binary_converter(impl_->schemas.value_at(converters.size()), impl_->profile));
        }
        return converters.at(index);
    }

    void BinaryEncodeSession::write(std::size_t index, const ValueView &view, std::string &out)
    {
        write(converter_at(index), view, out);
    }

    void BinaryEncodeSession::write(const BoundBinaryConverter &converter, const ValueView &view, std::string &out)
    {
        BinaryWriter writer{out, this};
        converter.write(view, writer);
    }

    void BinaryEncodeSession::write_tables(std::string &out) const { impl_->schemas.write(out); }

    // --- decode -------------------------------------------------------------

    struct BinaryDecodeSession::Impl
    {
        BinaryProfile profile{BinaryProfile::Compact};
        SchemaTableReader schemas{};
        // Bound on first use: an image names every schema its endpoints have,
        // and a reader that wants one of them should not pay to bind them all.
        std::vector<BoundBinaryConverter> converters{};
    };

    BinaryDecodeSession::BinaryDecodeSession(BinaryProfile profile) : impl_(std::make_unique<Impl>())
    {
        impl_->profile = profile;
    }

    BinaryDecodeSession::~BinaryDecodeSession() = default;

    BinaryProfile BinaryDecodeSession::profile() const noexcept { return impl_->profile; }

    void BinaryDecodeSession::read_tables(BinaryReader &reader)
    {
        impl_->schemas.read(reader);
        impl_->converters.assign(impl_->schemas.value_count(), BoundBinaryConverter{});
    }

    const SchemaTableReader &BinaryDecodeSession::schemas() const noexcept { return impl_->schemas; }

    const BoundBinaryConverter &BinaryDecodeSession::converter_at(std::size_t index)
    {
        if (index >= impl_->converters.size())
        {
            throw std::runtime_error(fmt::format("binary codec: value names schema {} of {}", index,
                                                 impl_->converters.size()));
        }
        auto &converter = impl_->converters[index];
        if (!converter) { converter = bind_binary_converter(impl_->schemas.value_at(index), impl_->profile); }
        return converter;
    }

    Value BinaryDecodeSession::read(std::size_t index, BinaryReader &reader)
    {
        return read(converter_at(index), reader);
    }

    Value BinaryDecodeSession::read(const BoundBinaryConverter &converter, BinaryReader &reader)
    {
        auto *const outer = reader.session;
        reader.session = this;
        auto restore = make_scope_exit([&]() noexcept { reader.session = outer; });
        return converter.read(reader);
    }

    // --- frame --------------------------------------------------------------

    std::uint8_t binary_profile_revision(BinaryProfile profile) noexcept
    {
        // Revision 0 of either profile is the RFC 0017 field-wise encoding.
        // Fast 1 (RFC 0040 stage 2): maps of fixed-width keys and values are
        // two blocks, and a list of composite rows is written by column.
        return profile == BinaryProfile::Fast ? 1 : 0;
    }

    void encode_binary_frame(const ValueView &view, BinaryProfile profile, std::string &out)
    {
        if (!view.valid()) { throw std::invalid_argument("binary codec: a frame needs a value"); }

        BinaryEncodeSession        session{profile};
        const BoundBinaryConverter root = bind_binary_converter(view.schema(), profile);

        out.push_back(static_cast<char>(profile));
        out.push_back(static_cast<char>(binary_profile_revision(profile)));
        const std::size_t length_at = out.size();
        out.append(sizeof(std::uint32_t), '\0');

        const std::size_t payload_at = out.size();
        session.write(root, view, out);
        const std::size_t payload = out.size() - payload_at;
        if (payload > UINT32_MAX) { throw std::length_error("binary codec: a frame's payload exceeds 4 GiB"); }
        for (std::size_t byte = 0; byte < sizeof(std::uint32_t); ++byte)
        {
            out[length_at + byte] = static_cast<char>((payload >> (8 * byte)) & 0xFFu);
        }

        session.write_tables(out);
    }

    std::string encode_binary_frame(const ValueView &view, BinaryProfile profile)
    {
        std::string out;
        encode_binary_frame(view, profile, out);
        return out;
    }

    BinaryProfile binary_frame_profile(std::string_view bytes)
    {
        if (bytes.size() < frame_header_bytes) { throw std::runtime_error("binary codec: truncated frame"); }
        return checked_profile(static_cast<std::uint8_t>(bytes[0]));
    }

    Value decode_binary_frame(const ValueTypeMetaData *meta, std::string_view bytes, BinaryDecodeLimits limits)
    {
        if (meta == nullptr) { throw std::logic_error("binary codec: null schema"); }
        const BinaryProfile profile = binary_frame_profile(bytes);
        const auto revision = static_cast<std::uint8_t>(bytes[1]);
        if (revision != binary_profile_revision(profile))
        {
            throw std::runtime_error(fmt::format("binary codec: frame is {} revision {}, this build reads revision {}",
                                                 profile_name(profile), revision,
                                                 binary_profile_revision(profile)));
        }

        std::uint32_t payload = 0;
        for (std::size_t byte = 0; byte < sizeof(std::uint32_t); ++byte)
        {
            payload |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[2 + byte])) << (8 * byte);
        }

        // One reader, so the payload and the tables draw on one budget. The
        // payload is stepped over first because the tables follow it.
        BinaryReader reader{bytes, frame_header_bytes, limits};
        BinaryReader values = reader.subreader(payload);

        BinaryDecodeSession session{profile};
        session.read_tables(reader);
        if (reader.remaining() != 0) { throw std::runtime_error("binary codec: trailing bytes after a frame"); }

        Value result = session.read(bind_binary_converter(meta, profile), values);
        if (values.remaining() != 0) { throw std::runtime_error("binary codec: trailing bytes after one value"); }
        return result;
    }
}  // namespace hgraph
