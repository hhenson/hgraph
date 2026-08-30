#ifndef HGRAPH_FABRIC_METADATA_CODEC_H
#define HGRAPH_FABRIC_METADATA_CODEC_H

#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <hgraph/persistence/value_store.h>
#include <hgraph/types/value/value.h>

#include <cstdint>
#include <span>

namespace hgraph::fabric
{
    /** Which index an object belongs to. Carried in the stored document so a
        latest entry read as an as-of entry is rejected rather than silently
        accepted; the check survived the move off the hand-written envelope
        because it is a correctness property, not a framing detail. */
    enum class MetadataObjectKind : std::uint8_t
    {
        Revision = 1,
        AsOf     = 2,
        Latest   = 3,
    };

    /** The DataRevision schema, for reads that name their type. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT const ValueTypeMetaData *data_revision_meta();

    /** The codec for transport payloads -- Kafka records and notifier blobs.
        Those are messages rather than stored objects, so they carry no key to
        name a format; they use the baseline json codec directly, which keeps a
        topic readable by an ordinary consumer. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT const persistence::store::ValueCodec &
    notification_codec();

    /** Encode an as-of or latest index entry as a RevisionReference document. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT persistence::store::ObjectBytes
    encode_reference(const persistence::store::ValueStore &values, MetadataObjectKind kind,
                     RevisionId revision);

    /** Decode an index entry and require its kind to match the object being
        read. Throws std::invalid_argument on a mismatch or malformed document. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT RevisionId
    revision_reference_value(const persistence::store::ValueStore &values,
                             MetadataObjectKind expected_kind,
                             std::span<const std::byte> encoded);

}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_METADATA_CODEC_H
