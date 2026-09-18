#ifndef HGRAPH_RUNTIME_CHECKPOINT_CODEC_H
#define HGRAPH_RUNTIME_CHECKPOINT_CODEC_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/types/value/binary_codec.h>
#include <hgraph/types/value/binary_compression.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace hgraph
{
    /**
     * Canonical byte form of checkpoint images (RFC 0039).
     *
     * A sibling of the RFC 0017 value codec, which it uses for every value.
     * It is the durable form a store publishes and the form a worker-hosted
     * graph returns to its owner, so it has no storage, publication or
     * discovery semantics of its own: ``hgraph-persistence`` owns those.
     *
     * Each distinct schema, node identifier and contract signature is written
     * once and referred to by index; a child endpoint names no schema when it
     * is the one its parent implies. Times are offsets from the enclosing
     * image. Decoding is bounded by the length of the input and verifies a
     * trailing checksum before it interprets anything.
     *
     * Version 3 (RFC 0040) records how its values are encoded -- the binary
     * profile and that profile's revision -- and holds everything after the
     * fixed header as one block that may be compressed. The checksum covers
     * the compressed bytes, so damage is found before a claimed length is
     * trusted. Version 2 images remain readable.
     *
     * Cold path only. Nothing here is reachable from evaluation.
     */
    inline constexpr std::uint32_t checkpoint_image_format_version = 3;

    /**
     * What an image is for decides how it is written (RFC 0040): one that is
     * stored is small and compressed; one handed to another process lives for
     * a cycle, so it is quick and never compressed.
     */
    struct HGRAPH_CLASS_EXPORT CheckpointImageOptions
    {
        BinaryProfile     profile{BinaryProfile::Compact};
        BinaryCompression compression{BinaryCompression::None};

        [[nodiscard]] static CheckpointImageOptions stored() noexcept
        {
            return {BinaryProfile::Compact, default_binary_compression()};
        }
        [[nodiscard]] static CheckpointImageOptions transport() noexcept
        {
            return {BinaryProfile::Fast, BinaryCompression::None};
        }
    };

    /**
     * The most a stored image may expand to when it is decompressed, unless a
     * reader says otherwise. The image's checksum detects damage; it is not a
     * MAC, so an image from a store that someone else can write to may claim
     * any size, and that claim sizes an allocation. A deployment with larger
     * state passes its own bound.
     */
    inline constexpr std::size_t checkpoint_image_default_max_bytes = std::size_t{4} << 30;   // 4 GiB

    /** Append the encoded image to ``out``, as a stored image unless told
     * otherwise. Throws before writing a value it cannot represent. */
    HGRAPH_EXPORT void encode_component_checkpoint(const ComponentCheckpoint &checkpoint, std::string &out);
    HGRAPH_EXPORT void encode_component_checkpoint(const ComponentCheckpoint &checkpoint, std::string &out,
                                                   const CheckpointImageOptions &options);
    [[nodiscard]] HGRAPH_EXPORT ComponentCheckpoint decode_component_checkpoint(std::string_view bytes);
    /** ``max_image_bytes`` bounds what the image may decompress to. */
    [[nodiscard]] HGRAPH_EXPORT ComponentCheckpoint decode_component_checkpoint(std::string_view bytes,
                                                                              std::size_t max_image_bytes);

    /** A graph image alone, as exchanged with a worker-hosted graph, so it is
     * a transport image unless told otherwise. Times are stored as offsets
     * from ``base_time``; any value round-trips, a time near the image's own
     * keeps the encoding short.
     */
    HGRAPH_EXPORT void encode_graph_checkpoint(const GraphCheckpointImage &graph, std::string &out,
                                               DateTime base_time = MIN_DT);
    HGRAPH_EXPORT void encode_graph_checkpoint(const GraphCheckpointImage &graph, std::string &out,
                                               DateTime base_time, const CheckpointImageOptions &options);
    [[nodiscard]] HGRAPH_EXPORT GraphCheckpointImage decode_graph_checkpoint(std::string_view bytes);
    [[nodiscard]] HGRAPH_EXPORT GraphCheckpointImage decode_graph_checkpoint(std::string_view bytes,
                                                                            std::size_t max_image_bytes);

    /** True when ``bytes`` begin with this codec's marker, whatever the version. */
    [[nodiscard]] HGRAPH_EXPORT bool is_checkpoint_image(std::string_view bytes) noexcept;
}

#endif
