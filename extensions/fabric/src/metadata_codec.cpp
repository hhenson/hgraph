#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/value_builders.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>

#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph::fabric
{
    namespace
    {
        [[nodiscard]] std::string_view kind_name(MetadataObjectKind kind)
        {
            switch (kind)
            {
                case MetadataObjectKind::Revision:
                    return "revision";
                case MetadataObjectKind::AsOf:
                    return "as_of";
                case MetadataObjectKind::Latest:
                    return "latest";
            }
            throw std::invalid_argument("unknown fabric metadata object kind");
        }

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
    encode_data_revision(const persistence::store::ValueStore &values,
                         const ValueView                      &revision)
    {
        auto encoded = values.encode(revision);
        require_metadata_within_limit(encoded.size());
        return encoded;
    }

    Value decode_data_revision(const persistence::store::ValueStore &values,
                               std::span<const std::byte>            encoded)
    {
        require_metadata_within_limit(encoded.size());
        Value decoded = values.decode(data_revision_meta(), encoded);
        validate_data_revision(data_revision_input(decoded.view()));
        return decoded;
    }

    const persistence::store::ValueCodec &notification_codec()
    {
        static const persistence::store::ValueCodec codec = [] {
            persistence::store::register_builtin_value_codecs();
            return persistence::store::value_codec(persistence::store::JSON_VALUE_CODEC);
        }();
        return codec;
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
        BundleBuilder builder{
            ValuePlanFactory::instance().type_for(revision_reference_meta())};
        if (kind != MetadataObjectKind::AsOf && kind != MetadataObjectKind::Latest)
        {
            throw std::invalid_argument(
                "fabric revision reference kind must be as-of or latest");
        }
        require_positive_ordinal(revision, "revision reference");
        builder.set("kind", Value{Str{kind_name(kind)}}.view());
        builder.set("revision", Value{revision}.view());
        return builder.build();
    }

    RevisionId revision_reference_id(const ValueView   &reference,
                                     MetadataObjectKind expected_kind)
    {
        const auto fields = reference.as_bundle();
        const auto kind   = fields.at("kind");
        const auto revision = fields.at("revision");
        if (kind.data() == nullptr || revision.data() == nullptr)
        {
            throw std::invalid_argument("fabric revision reference is incomplete");
        }
        const auto stored_kind = kind.checked_as<Str>();
        if (stored_kind != kind_name(expected_kind))
        {
            // The index a document belongs to is part of its meaning: a latest
            // entry read as an as-of entry would silently answer the wrong
            // question.
            throw std::invalid_argument("fabric revision reference is a '" +
                                        std::string{stored_kind} + "' entry, expected '" +
                                        std::string{kind_name(expected_kind)} + "'");
        }
        const auto stored_revision = revision.checked_as<Int>();
        require_positive_ordinal(stored_revision, "revision reference");
        return stored_revision;
    }

    persistence::store::ObjectBytes encode_reference(
        const persistence::store::ValueStore &values, MetadataObjectKind kind,
        RevisionId revision)
    {
        return values.encode(make_revision_reference(kind, revision).view());
    }

    RevisionId revision_reference_value(const persistence::store::ValueStore &values,
                                        MetadataObjectKind         expected_kind,
                                        std::span<const std::byte> encoded)
    {
        return revision_reference_id(
            values.decode(revision_reference_meta(), encoded).view(), expected_kind);
    }

}  // namespace hgraph::fabric
