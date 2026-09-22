#ifndef HGRAPH_TYPES_VALUE_BINARY_COMPRESSION_H
#define HGRAPH_TYPES_VALUE_BINARY_COMPRESSION_H

// Block compression for bytes that are stored (RFC 0040).
//
// Checkpoints, recordings and store objects are written once and read rarely,
// so they are compressed by default; bytes that cross a process boundary live
// for one cycle and never are. The codec is Arrow's, which the core already
// links, so this adds no dependency: zstd where the Arrow build provides it,
// LZ4 otherwise.
//
// A block is self-describing:
//
//   u8      codec           0 none, 1 zstd, 2 lz4 (frame format)
//   varint  stored length
//   varint  raw length      only when the codec is not none
//           bytes
//
// so a reader never guesses, and one without the codec refuses by name.

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/binary_codec.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace hgraph
{
    enum class BinaryCompression : std::uint8_t
    {
        None = 0,
        Zstd = 1,
        Lz4 = 2,
    };

    /** Below this a block is stored as it is: the codec's own framing costs more than it saves. */
    inline constexpr std::size_t binary_compression_threshold = 256;

    /** What a reader will allocate for one block unless told otherwise. It is
        deliberately modest: whoever owns the bytes knows how large they may
        honestly be, and should say so. */
    inline constexpr std::size_t binary_compression_default_max_raw_bytes = std::size_t{1} << 30;   // 1 GiB

    /** zstd when this build's Arrow provides it, LZ4 otherwise, none when neither. */
    [[nodiscard]] HGRAPH_EXPORT BinaryCompression default_binary_compression() noexcept;
    [[nodiscard]] HGRAPH_EXPORT bool binary_compression_available(BinaryCompression compression) noexcept;
    [[nodiscard]] HGRAPH_EXPORT std::string_view binary_compression_name(BinaryCompression compression) noexcept;

    /**
     * Append ``raw`` as one block. It is stored uncompressed when it is below
     * the threshold, when the codec is unavailable in this build, or when
     * compressing it would not make it smaller.
     */
    HGRAPH_EXPORT void write_compressed_block(std::string_view raw, BinaryCompression compression, std::string &out);

    /**
     * Read one block. The result views ``storage`` when the block had to be
     * decompressed and the reader's own buffer when it did not, so an
     * uncompressed block costs no copy.
     *
     * The raw length is the writer's claim, and it sizes an allocation, so it
     * is bounded by ``max_raw_bytes`` before anything is allocated for it. A
     * checksum that anyone can recompute does not make the claim honest: the
     * owner of the bytes passes the most they could honestly expand to.
     */
    [[nodiscard]] HGRAPH_EXPORT std::string_view read_compressed_block(
        BinaryReader &reader, std::string &storage,
        std::size_t max_raw_bytes = binary_compression_default_max_raw_bytes);
}  // namespace hgraph

#endif  // HGRAPH_TYPES_VALUE_BINARY_COMPRESSION_H
