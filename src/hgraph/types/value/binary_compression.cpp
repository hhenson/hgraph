#include <hgraph/types/value/binary_compression.h>

#include <arrow/util/compression.h>

#include <fmt/format.h>

#include <memory>
#include <stdexcept>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] arrow::Compression::type arrow_kind(BinaryCompression compression)
        {
            switch (compression)
            {
                case BinaryCompression::Zstd: return arrow::Compression::ZSTD;
                case BinaryCompression::Lz4: return arrow::Compression::LZ4_FRAME;
                case BinaryCompression::None: break;
            }
            return arrow::Compression::UNCOMPRESSED;
        }

        [[nodiscard]] std::unique_ptr<arrow::util::Codec> make_codec(BinaryCompression compression)
        {
            auto codec = arrow::util::Codec::Create(arrow_kind(compression));
            if (!codec.ok())
            {
                throw std::runtime_error(fmt::format("binary codec: {} compression is unavailable: {}",
                                                     binary_compression_name(compression), codec.status().ToString()));
            }
            return std::move(codec).ValueUnsafe();
        }

        void write_stored(std::string_view raw, std::string &out)
        {
            out.push_back(static_cast<char>(BinaryCompression::None));
            write_varint(raw.size(), out);
            out.append(raw);
        }
    }  // namespace

    bool binary_compression_available(BinaryCompression compression) noexcept
    {
        return compression == BinaryCompression::None || arrow::util::Codec::IsAvailable(arrow_kind(compression));
    }

    BinaryCompression default_binary_compression() noexcept
    {
        if (binary_compression_available(BinaryCompression::Zstd)) { return BinaryCompression::Zstd; }
        if (binary_compression_available(BinaryCompression::Lz4)) { return BinaryCompression::Lz4; }
        return BinaryCompression::None;
    }

    std::string_view binary_compression_name(BinaryCompression compression) noexcept
    {
        switch (compression)
        {
            case BinaryCompression::None: return "none";
            case BinaryCompression::Zstd: return "zstd";
            case BinaryCompression::Lz4: return "lz4";
        }
        return "unknown";
    }

    void write_compressed_block(std::string_view raw, BinaryCompression compression, std::string &out)
    {
        if (compression == BinaryCompression::None || raw.size() < binary_compression_threshold ||
            !binary_compression_available(compression))
        {
            write_stored(raw, out);
            return;
        }

        const auto codec = make_codec(compression);
        const auto *input = reinterpret_cast<const std::uint8_t *>(raw.data());
        const auto  bound = codec->MaxCompressedLen(static_cast<std::int64_t>(raw.size()), input);

        // Compress straight into the output behind a provisional header, and
        // fall back to storing the bytes if that did not make them smaller.
        const std::size_t start = out.size();
        out.push_back(static_cast<char>(compression));
        std::string header_tail;   // lengths are only known after compressing
        const std::size_t scratch_at = out.size();
        out.resize(scratch_at + static_cast<std::size_t>(bound));
        const auto written = codec->Compress(static_cast<std::int64_t>(raw.size()), input, bound,
                                             reinterpret_cast<std::uint8_t *>(out.data() + scratch_at));
        if (!written.ok())
        {
            out.resize(start);
            throw std::runtime_error(fmt::format("binary codec: {} compression failed: {}",
                                                 binary_compression_name(compression), written.status().ToString()));
        }
        const auto stored = static_cast<std::size_t>(*written);
        write_varint(stored, header_tail);
        write_varint(raw.size(), header_tail);
        if (stored + header_tail.size() >= raw.size())
        {
            out.resize(start);
            write_stored(raw, out);
            return;
        }
        out.resize(scratch_at + stored);
        out.insert(scratch_at, header_tail);
    }

    std::string_view read_compressed_block(BinaryReader &reader, std::string &storage, std::size_t max_raw_bytes)
    {
        const auto kind = std::to_integer<std::uint8_t>(*reader.take(1));
        if (kind > static_cast<std::uint8_t>(BinaryCompression::Lz4))
        {
            throw std::runtime_error(fmt::format("binary codec: block names unknown compression {}", kind));
        }
        const auto compression = static_cast<BinaryCompression>(kind);
        const auto stored = static_cast<std::size_t>(read_varint(reader));
        if (compression == BinaryCompression::None)
        {
            const auto *bytes = reader.take(stored);
            return {reinterpret_cast<const char *>(bytes), stored};
        }

        const auto raw = read_varint(reader);
        if (raw > max_raw_bytes)
        {
            throw std::runtime_error(fmt::format("binary codec: block claims {} bytes, more than the {} allowed", raw,
                                                 max_raw_bytes));
        }
        if (!binary_compression_available(compression))
        {
            throw std::runtime_error(fmt::format("binary codec: block is {} compressed and this build cannot read it",
                                                 binary_compression_name(compression)));
        }
        const auto *bytes = reader.take(stored);
        const auto  codec = make_codec(compression);
        storage.resize(static_cast<std::size_t>(raw));
        const auto read = codec->Decompress(static_cast<std::int64_t>(stored), reinterpret_cast<const std::uint8_t *>(bytes),
                                            static_cast<std::int64_t>(raw),
                                            reinterpret_cast<std::uint8_t *>(storage.data()));
        if (!read.ok())
        {
            throw std::runtime_error(fmt::format("binary codec: {} block is damaged: {}",
                                                 binary_compression_name(compression), read.status().ToString()));
        }
        if (static_cast<std::uint64_t>(*read) != raw)
        {
            throw std::runtime_error("binary codec: block length disagrees with its contents");
        }
        return storage;
    }
}  // namespace hgraph
