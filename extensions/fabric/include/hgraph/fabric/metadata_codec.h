#ifndef HGRAPH_FABRIC_METADATA_CODEC_H
#define HGRAPH_FABRIC_METADATA_CODEC_H

#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <hgraph/persistence/value_codec.h>
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
    [[nodiscard]] HGRAPH_FABRIC_EXPORT persistence::store::ValueCodec
    notification_codec();

    /** The RevisionReference schema, for reads that name their type. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT const ValueTypeMetaData *revision_reference_meta();

    /** Ad-hoc as-of/latest index builder. Run-local code uses its private
        retained binding; the store decides how values are encoded. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Value
    make_revision_reference(MetadataObjectKind kind, RevisionId revision);

    /** Ad-hoc index reader, requiring its kind to match the object being read.
        Run-local code uses the retained binding. Throws std::invalid_argument
        on a mismatch or malformed entry. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT RevisionId
    revision_reference_id(const ValueView &reference, MetadataObjectKind expected_kind);

    /** Encode a DataRevision through `codec`, enforcing Fabric's aggregate
        metadata limit (MAX_METADATA_BYTES). The store owns the format; the
        limit is Fabric's own invariant and survived the move off the
        hand-written codec. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT persistence::store::ObjectBytes
    encode_data_revision(const persistence::store::BoundValueCodec &codec,
                         const ValueView                           &revision);

    /** Ad-hoc decode through `codec`, including Fabric validation. Graph code
        uses its private run-local value binding instead. The documents are
        externally editable, so every field is checked rather than trusted. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Value
    decode_data_revision(const persistence::store::BoundValueCodec &codec,
                         std::span<const std::byte>                 encoded);

    /** Validate a decoded revision. Exposed so call sites that already hold a
        Value (a Kafka payload, a notification) get the same checks. */
    HGRAPH_FABRIC_EXPORT void validate_data_revision(const DataRevisionInput &revision);

    /** Fabric's aggregate metadata limit. Applies to notification payloads as
        well as stored objects: a document that is too large to persist is also
        too large to put on a topic. */
    HGRAPH_FABRIC_EXPORT void require_metadata_within_limit(std::size_t size);

    /** Ad-hoc index-entry encode through `codec`. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT persistence::store::ObjectBytes
    encode_reference(const persistence::store::BoundValueCodec &codec,
                     MetadataObjectKind kind, RevisionId revision);

    /** Ad-hoc index-entry decode through `codec`, checking its kind. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT RevisionId
    revision_reference_value(const persistence::store::BoundValueCodec &codec,
                             MetadataObjectKind         expected_kind,
                             std::span<const std::byte> encoded);

}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_METADATA_CODEC_H
