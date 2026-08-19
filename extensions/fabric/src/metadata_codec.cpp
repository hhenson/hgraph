#include <hgraph/fabric/metadata_codec.h>

#include <hgraph/fabric/value_builders.h>

#include <bit>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph::fabric
{
    namespace
    {
        inline constexpr std::uint16_t SELF_PREDECESSOR_FLAG{0x0001U};

        class Writer
        {
          public:
            void byte(std::uint8_t value)
            {
                if (bytes_.size() == MAX_METADATA_BYTES)
                {
                    throw std::invalid_argument("fabric metadata exceeds 16 MiB");
                }
                bytes_.push_back(static_cast<std::byte>(value));
            }

            void u16(std::uint16_t value)
            {
                byte(static_cast<std::uint8_t>(value >> 8U));
                byte(static_cast<std::uint8_t>(value));
            }

            void u32(std::uint32_t value)
            {
                for (int shift = 24; shift >= 0; shift -= 8)
                {
                    byte(static_cast<std::uint8_t>(value >> shift));
                }
            }

            void u64(std::uint64_t value)
            {
                for (int shift = 56; shift >= 0; shift -= 8)
                {
                    byte(static_cast<std::uint8_t>(value >> shift));
                }
            }

            void string(std::string_view value)
            {
                if (value.size() > std::numeric_limits<std::uint32_t>::max())
                {
                    throw std::invalid_argument("fabric metadata string is too large");
                }
                u32(static_cast<std::uint32_t>(value.size()));
                for (const unsigned char character : value) { byte(character); }
            }

            [[nodiscard]] persistence::store::ObjectBytes finish()
            {
                if (bytes_.size() > MAX_METADATA_BYTES)
                {
                    throw std::invalid_argument("fabric metadata exceeds 16 MiB");
                }
                return std::move(bytes_);
            }

          private:
            persistence::store::ObjectBytes bytes_{};
        };

        class Reader
        {
          public:
            explicit Reader(std::span<const std::byte> bytes) : bytes_(bytes)
            {
                if (bytes.size() > MAX_METADATA_BYTES)
                {
                    throw std::invalid_argument("fabric metadata exceeds 16 MiB");
                }
            }

            [[nodiscard]] std::uint8_t byte()
            {
                require(1);
                return std::to_integer<std::uint8_t>(bytes_[offset_++]);
            }

            [[nodiscard]] std::uint16_t u16()
            {
                std::uint16_t value{};
                for (int index = 0; index < 2; ++index)
                {
                    value = static_cast<std::uint16_t>((value << 8U) | byte());
                }
                return value;
            }

            [[nodiscard]] std::uint32_t u32()
            {
                std::uint32_t value{};
                for (int index = 0; index < 4; ++index)
                {
                    value = (value << 8U) | byte();
                }
                return value;
            }

            [[nodiscard]] std::uint64_t u64()
            {
                std::uint64_t value{};
                for (int index = 0; index < 8; ++index)
                {
                    value = (value << 8U) | byte();
                }
                return value;
            }

            [[nodiscard]] Str string()
            {
                const std::uint32_t size = u32();
                require(size);
                const char *start = reinterpret_cast<const char *>(
                    bytes_.data() + static_cast<std::ptrdiff_t>(offset_));
                Str value{start, size};
                offset_ += size;
                require_data_id(value);
                return value;
            }

            void finish() const
            {
                if (offset_ != bytes_.size())
                {
                    throw std::invalid_argument(
                        "fabric metadata contains trailing bytes");
                }
            }

          private:
            void require(std::size_t count) const
            {
                if (count > bytes_.size() - offset_)
                {
                    throw std::invalid_argument("truncated fabric metadata");
                }
            }

            std::span<const std::byte> bytes_{};
            std::size_t                offset_{};
        };

        void write_header(Writer &writer, MetadataObjectKind kind,
                          std::uint16_t flags)
        {
            writer.byte('H');
            writer.byte('G');
            writer.byte('F');
            writer.byte('M');
            writer.byte(1);
            writer.byte(static_cast<std::uint8_t>(kind));
            writer.u16(flags);
        }

        [[nodiscard]] std::uint16_t read_header(Reader &reader,
                                                MetadataObjectKind expected)
        {
            if (reader.byte() != 'H' || reader.byte() != 'G' ||
                reader.byte() != 'F' || reader.byte() != 'M')
            {
                throw std::invalid_argument("fabric metadata has invalid magic");
            }
            if (reader.byte() != 1)
            {
                throw std::invalid_argument("unsupported fabric metadata version");
            }
            if (reader.byte() != static_cast<std::uint8_t>(expected))
            {
                throw std::invalid_argument("fabric metadata object kind mismatch");
            }
            return reader.u16();
        }

        void require_positive_ordinal(std::uint64_t value, std::string_view field)
        {
            if (value == 0 ||
                value > static_cast<std::uint64_t>(std::numeric_limits<Int>::max()))
            {
                throw std::invalid_argument("fabric " + std::string{field} +
                                            " is out of range");
            }
        }
    }  // namespace

    persistence::store::ObjectBytes encode_revision(ValueView revision)
    {
        const DataRevisionInput input = data_revision_input(std::move(revision));
        Writer writer;
        write_header(writer, MetadataObjectKind::Revision,
                     input.self_predecessor.has_value()
                         ? SELF_PREDECESSOR_FLAG
                         : std::uint16_t{});
        writer.u64(static_cast<std::uint64_t>(input.revision));
        writer.u64(static_cast<std::uint64_t>(input.output_version));
        writer.u64(std::bit_cast<std::uint64_t>(
            static_cast<std::int64_t>(input.as_of.time_since_epoch().count())));
        writer.string(input.data_id);
        writer.u32(static_cast<std::uint32_t>(input.dependencies.size()));
        for (const auto &dependency : input.dependencies)
        {
            writer.string(dependency.data_id);
            writer.u64(static_cast<std::uint64_t>(dependency.version));
        }
        if (input.self_predecessor.has_value())
        {
            writer.u64(static_cast<std::uint64_t>(*input.self_predecessor));
        }
        return writer.finish();
    }

    Value decode_revision(std::span<const std::byte> encoded)
    {
        Reader reader{encoded};
        const std::uint16_t flags = read_header(reader, MetadataObjectKind::Revision);
        if ((flags & ~SELF_PREDECESSOR_FLAG) != 0)
        {
            throw std::invalid_argument("fabric revision contains unknown flags");
        }

        const std::uint64_t revision = reader.u64();
        const std::uint64_t output_version = reader.u64();
        require_positive_ordinal(revision, "revision id");
        require_positive_ordinal(output_version, "output version");
        const auto raw_as_of = std::bit_cast<std::int64_t>(reader.u64());
        Str data_id = reader.string();
        const std::uint32_t count = reader.u32();
        if (count > MAX_REVISION_DEPENDENCIES)
        {
            throw std::invalid_argument("fabric revision has too many dependencies");
        }

        std::vector<DataDependencyInput> dependencies;
        dependencies.reserve(count);
        for (std::uint32_t index = 0; index < count; ++index)
        {
            Str dependency_id = reader.string();
            const std::uint64_t dependency_version = reader.u64();
            require_positive_ordinal(dependency_version, "dependency version");
            if (!dependencies.empty() &&
                !canonical_data_id_less(dependencies.back().data_id,
                                        dependency_id))
            {
                throw std::invalid_argument(
                    "fabric revision dependencies are not canonical");
            }
            dependencies.push_back(DataDependencyInput{
                .data_id = std::move(dependency_id),
                .version = static_cast<Int>(dependency_version),
            });
        }
        std::optional<DataVersion> self_predecessor;
        if ((flags & SELF_PREDECESSOR_FLAG) != 0)
        {
            const std::uint64_t raw_predecessor = reader.u64();
            require_positive_ordinal(raw_predecessor, "self predecessor");
            self_predecessor = static_cast<Int>(raw_predecessor);
        }
        reader.finish();

        return make_data_revision(DataRevisionInput{
            .format_version = REVISION_FORMAT_VERSION,
            .data_id = std::move(data_id),
            .revision = static_cast<Int>(revision),
            .output_version = static_cast<Int>(output_version),
            .dependencies = std::move(dependencies),
            .self_predecessor = self_predecessor,
            .as_of = DateTime{TimeDelta{raw_as_of}},
        });
    }

    persistence::store::ObjectBytes encode_revision_reference(
        MetadataObjectKind kind, RevisionId revision)
    {
        if (kind != MetadataObjectKind::AsOf && kind != MetadataObjectKind::Latest)
        {
            throw std::invalid_argument(
                "fabric revision reference kind must be as-of or latest");
        }
        if (revision <= 0)
        {
            throw std::invalid_argument("fabric revision reference must be positive");
        }
        Writer writer;
        write_header(writer, kind, 0);
        writer.u64(static_cast<std::uint64_t>(revision));
        return writer.finish();
    }

    RevisionId decode_revision_reference(MetadataObjectKind expected_kind,
                                         std::span<const std::byte> encoded)
    {
        if (expected_kind != MetadataObjectKind::AsOf &&
            expected_kind != MetadataObjectKind::Latest)
        {
            throw std::invalid_argument(
                "fabric revision reference kind must be as-of or latest");
        }
        Reader reader{encoded};
        if (read_header(reader, expected_kind) != 0)
        {
            throw std::invalid_argument(
                "fabric revision reference contains unknown flags");
        }
        const std::uint64_t revision = reader.u64();
        require_positive_ordinal(revision, "revision reference");
        reader.finish();
        return static_cast<RevisionId>(revision);
    }
}  // namespace hgraph::fabric
