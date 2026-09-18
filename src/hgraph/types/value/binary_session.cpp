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

    void BinaryEncodeSession::write_run(std::size_t index, std::span<const Value> values, std::string &out)
    {
        BinaryWriter writer{out, this};
        converter_at(index).write_run(values, writer);
    }

    void BinaryEncodeSession::write_tables(std::string &out) const { impl_->schemas.write(out); }

    // --- decode -------------------------------------------------------------

    struct BinaryDecodeSession::Impl
    {
        BinaryProfile profile{BinaryProfile::Compact};
        std::uint8_t revision{0};
        SchemaTableReader schemas{};
        // Bound on first use: an image names every schema its endpoints have,
        // and a reader that wants one of them should not pay to bind them all.
        std::vector<BoundBinaryConverter> converters{};
    };

    BinaryDecodeSession::BinaryDecodeSession(BinaryProfile profile)
        : BinaryDecodeSession(profile, binary_profile_revision(profile))
    {
    }

    BinaryDecodeSession::BinaryDecodeSession(BinaryProfile profile, std::uint8_t revision)
        : impl_(std::make_unique<Impl>())
    {
        impl_->profile = profile;
        impl_->revision = revision;
    }

    BinaryDecodeSession::~BinaryDecodeSession() = default;

    BinaryProfile BinaryDecodeSession::profile() const noexcept { return impl_->profile; }

    std::uint8_t BinaryDecodeSession::revision() const noexcept { return impl_->revision; }

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
        if (!converter)
        {
            converter = bind_binary_converter(impl_->schemas.value_at(index), impl_->profile, impl_->revision);
        }
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

    void encode_binary_frame(const BoundBinaryConverter &root, const ValueView &view, std::string &out)
    {
        if (!root) { throw std::logic_error("binary codec: unbound converter"); }
        if (!view.valid()) { throw std::invalid_argument("binary codec: a frame needs a value"); }

        BinaryEncodeSession session{root.profile()};
        out.push_back(static_cast<char>(root.profile()));
        out.push_back(static_cast<char>(root.revision()));
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

    void encode_binary_frame(const ValueView &view, BinaryProfile profile, std::string &out)
    {
        if (!view.valid()) { throw std::invalid_argument("binary codec: a frame needs a value"); }
        encode_binary_frame(bind_binary_converter(view.schema(), profile), view, out);
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

    namespace
    {
        struct OpenedFrame
        {
            BinaryProfile profile;
            std::uint8_t  revision;
        };

        [[nodiscard]] OpenedFrame open_frame(std::string_view bytes)
        {
            const BinaryProfile profile = binary_frame_profile(bytes);
            const auto revision = static_cast<std::uint8_t>(bytes[1]);
            if (revision > binary_profile_revision(profile))
            {
                throw std::runtime_error(fmt::format(
                    "binary codec: frame is {} revision {}, this build reads revision {}", profile_name(profile),
                    revision, binary_profile_revision(profile)));
            }
            return {profile, revision};
        }

        [[nodiscard]] Value read_frame_body(const BoundBinaryConverter &root, std::string_view bytes,
                                            BinaryDecodeLimits limits)
        {
            std::uint32_t payload = 0;
            for (std::size_t byte = 0; byte < sizeof(std::uint32_t); ++byte)
            {
                payload |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[2 + byte])) << (8 * byte);
            }

            // One reader, so the payload and the tables draw on one budget. The
            // payload is stepped over first because the tables follow it.
            BinaryReader reader{bytes, frame_header_bytes, limits};
            BinaryReader values = reader.subreader(payload);

            BinaryDecodeSession session{root.profile(), root.revision()};
            session.read_tables(reader);
            if (reader.remaining() != 0) { throw std::runtime_error("binary codec: trailing bytes after a frame"); }

            Value result = session.read(root, values);
            if (values.remaining() != 0) { throw std::runtime_error("binary codec: trailing bytes after one value"); }
            return result;
        }
    }  // namespace

    Value decode_binary_frame(const BoundBinaryConverter &root, std::string_view bytes, BinaryDecodeLimits limits)
    {
        if (!root) { throw std::logic_error("binary codec: unbound converter"); }
        const auto frame = open_frame(bytes);
        if (frame.profile != root.profile())
        {
            throw std::runtime_error(fmt::format("binary codec: frame is {}, and this reader is bound for {}",
                                                 profile_name(frame.profile), profile_name(root.profile())));
        }
        if (frame.revision == root.revision()) { return read_frame_body(root, bytes, limits); }
        // Stored bytes of an older revision: bind that revision for this read.
        return read_frame_body(bind_binary_converter(root.schema(), frame.profile, frame.revision), bytes, limits);
    }

    Value decode_binary_frame(const ValueTypeMetaData *meta, std::string_view bytes, BinaryDecodeLimits limits)
    {
        if (meta == nullptr) { throw std::logic_error("binary codec: null schema"); }
        const auto frame = open_frame(bytes);
        return read_frame_body(bind_binary_converter(meta, frame.profile, frame.revision), bytes, limits);
    }
}  // namespace hgraph
