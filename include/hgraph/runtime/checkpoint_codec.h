#ifndef HGRAPH_RUNTIME_CHECKPOINT_CODEC_H
#define HGRAPH_RUNTIME_CHECKPOINT_CODEC_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/component_checkpoint.h>

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
     * Cold path only. Nothing here is reachable from evaluation.
     */
    inline constexpr std::uint32_t checkpoint_image_format_version = 2;

    /** Append the encoded image to ``out``. Throws before writing a value it cannot represent. */
    HGRAPH_EXPORT void encode_component_checkpoint(const ComponentCheckpoint &checkpoint, std::string &out);
    [[nodiscard]] HGRAPH_EXPORT ComponentCheckpoint decode_component_checkpoint(std::string_view bytes);

    /** A graph image alone, as exchanged with a worker-hosted graph. Times are
     * stored as offsets from ``base_time``; any value round-trips, a time near
     * the image's own keeps the encoding short.
     */
    HGRAPH_EXPORT void encode_graph_checkpoint(const GraphCheckpointImage &graph, std::string &out,
                                               DateTime base_time = MIN_DT);
    [[nodiscard]] HGRAPH_EXPORT GraphCheckpointImage decode_graph_checkpoint(std::string_view bytes);

    /** True when ``bytes`` begin with this codec's marker, whatever the version. */
    [[nodiscard]] HGRAPH_EXPORT bool is_checkpoint_image(std::string_view bytes) noexcept;
}

#endif
