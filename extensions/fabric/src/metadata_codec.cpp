#include <hgraph/fabric/metadata_codec.h>

#include <hgraph/fabric/value_builders.h>

#include "impl/metadata_value_binding.h"

#include <hgraph/types/static_schema.h>

#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph::fabric
{
    namespace
    {
        /** The hand-written codec rejected zero, negative and out-of-range
            ordinals at the decode boundary. A json document is easier to hand
            edit than a binary blob, not harder, so the check belongs here
            still. */
        void require_positive_ordinal(Int value, std::string_view field)
        {
            if (value <= 0)
            {
                throw std::invalid_argument("fabric " + std::string{field} +
                                            " is out of range");
            }
        }

    }  // namespace

    void require_metadata_within_limit(std::size_t size)
    {
        if (size > MAX_METADATA_BYTES)
        {
            throw std::invalid_argument("fabric metadata exceeds 16 MiB");
        }
    }

    void validate_data_revision(const DataRevisionInput &revision)
    {
        require_positive_ordinal(revision.revision, "revision id");
        require_positive_ordinal(revision.output_version, "output version");
        if (revision.dependencies.size() > MAX_REVISION_DEPENDENCIES)
        {
            throw std::invalid_argument("fabric revision has too many dependencies");
        }
        for (std::size_t index = 0; index < revision.dependencies.size(); ++index)
        {
            const auto &dependency = revision.dependencies[index];
            require_positive_ordinal(dependency.version, "dependency version");
            // Canonical order is what makes two equivalent revisions compare
            // equal, so an out-of-order document is malformed rather than
            // merely unusual.
            if (index > 0 &&
                !canonical_data_id_less(revision.dependencies[index - 1].data_id,
                                        dependency.data_id))
            {
                throw std::invalid_argument(
                    "fabric revision dependencies are not canonical");
            }
        }
        if (revision.self_predecessor.has_value())
        {
            require_positive_ordinal(*revision.self_predecessor, "self predecessor");
        }
    }

    persistence::store::ObjectBytes
    encode_data_revision(const persistence::store::BoundValueCodec &codec,
                         const ValueView                           &revision)
    {
        auto encoded = codec.encode(revision);
        require_metadata_within_limit(encoded.size());
        return encoded;
    }

    Value decode_data_revision(const persistence::store::BoundValueCodec &codec,
                               std::span<const std::byte>                 encoded)
    {
        require_metadata_within_limit(encoded.size());
        Value decoded = codec.decode(encoded);
        const detail::FabricMetadataValueBinding values;
        validate_data_revision(values.data_revision_input(decoded.view()));
        return decoded;
    }

    persistence::store::ValueCodec notification_codec()
    {
        return persistence::store::value_codec(
            persistence::store::JSON_VALUE_CODEC);
    }

    const ValueTypeMetaData *data_revision_meta()
    {
        return scalar_descriptor<DataRevision>::value_meta();
    }

    const ValueTypeMetaData *revision_reference_meta()
    {
        return scalar_descriptor<RevisionReference>::value_meta();
    }

    Value make_revision_reference(MetadataObjectKind kind, RevisionId revision)
    {
        return detail::FabricMetadataValueBinding{}.make_revision_reference(
            kind, revision);
    }

    RevisionId revision_reference_id(const ValueView   &reference,
                                     MetadataObjectKind expected_kind)
    {
        return detail::FabricMetadataValueBinding{}.revision_reference_id(
            ValueView{reference.binding(), reference.data()}, expected_kind);
    }

    persistence::store::ObjectBytes encode_reference(
        const persistence::store::BoundValueCodec &codec,
        MetadataObjectKind kind, RevisionId revision)
    {
        const detail::FabricMetadataValueBinding values;
        auto encoded =
            codec.encode(values.make_revision_reference(kind, revision).view());
        require_metadata_within_limit(encoded.size());
        return encoded;
    }

    RevisionId revision_reference_value(const persistence::store::BoundValueCodec &codec,
                                        MetadataObjectKind         expected_kind,
                                        std::span<const std::byte> encoded)
    {
        require_metadata_within_limit(encoded.size());
        const detail::FabricMetadataValueBinding values;
        return values.revision_reference_id(codec.decode(encoded).view(),
                                            expected_kind);
    }

}  // namespace hgraph::fabric
