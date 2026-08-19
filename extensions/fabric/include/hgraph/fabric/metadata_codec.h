#ifndef HGRAPH_FABRIC_METADATA_CODEC_H
#define HGRAPH_FABRIC_METADATA_CODEC_H

#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <hgraph/persistence/object_store.h>
#include <hgraph/types/value/value.h>

#include <cstdint>
#include <span>
#include <string_view>

namespace hgraph::fabric
{
    enum class MetadataObjectKind : std::uint8_t
    {
        Revision = 1,
        AsOf     = 2,
        Latest   = 3,
    };

    inline constexpr std::string_view REVISION_MEDIA_TYPE{
        "application/vnd.hgraph.fabric.revision.v1+binary"};
    inline constexpr std::string_view AS_OF_MEDIA_TYPE{
        "application/vnd.hgraph.fabric.as-of.v1+binary"};
    inline constexpr std::string_view LATEST_MEDIA_TYPE{
        "application/vnd.hgraph.fabric.latest.v1+binary"};

    /** Encode a validated DataRevision using the RFC 0026 canonical v1 binary
        envelope. Multi-byte integers are big-endian and no alternate field
        order or trailing bytes are accepted. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT persistence::store::ObjectBytes
    encode_revision(ValueView revision);

    /** Decode one canonical revision envelope. Unknown versions, malformed
        UTF-8, non-canonical dependencies, invalid ordinals, bounds violations,
        and trailing bytes fail closed with std::invalid_argument. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Value
    decode_revision(std::span<const std::byte> encoded);

    /** Encode the revision id stored by an as-of entry or latest reference. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT persistence::store::ObjectBytes
    encode_revision_reference(MetadataObjectKind kind, RevisionId revision);

    /** Decode a reference and require its envelope kind to match the object
        path being read. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT RevisionId
    decode_revision_reference(MetadataObjectKind expected_kind,
                              std::span<const std::byte> encoded);
}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_METADATA_CODEC_H
