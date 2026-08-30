#include <hgraph/fabric/metadata_codec.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>

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
    }  // namespace

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
        return revision.checked_as<Int>();
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
