#include <hgraph/fabric/value_builders.h>

#include <hgraph/fabric/metadata_codec.h>

#include "impl/metadata_value_binding.h"

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace hgraph::fabric
{
    namespace
    {
        template <typename T>
        [[nodiscard]] Value atomic(const ValueTypeRef &binding, const T &value)
        {
            return Value{binding, std::addressof(value)};
        }

        [[nodiscard]] std::string_view reference_kind_name(MetadataObjectKind kind)
        {
            switch (kind)
            {
                case MetadataObjectKind::AsOf:
                    return "as_of";
                case MetadataObjectKind::Latest:
                    return "latest";
                case MetadataObjectKind::Revision:
                    break;
            }
            throw std::invalid_argument(
                "fabric revision reference kind must be as-of or latest");
        }

        void require_positive(Int value, std::string_view field)
        {
            if (value <= 0)
            {
                throw std::invalid_argument("fabric " + std::string{field} +
                                            " must be positive");
            }
        }

        void validate_revision_header(const DataRevisionInput &revision)
        {
            if (revision.format_version != REVISION_FORMAT_VERSION)
            {
                throw std::invalid_argument("unsupported fabric revision format version");
            }
            require_data_id(revision.data_id);
            require_positive(revision.revision, "revision id");
            require_positive(revision.output_version, "output version");
            if (revision.dependencies.size() > MAX_REVISION_DEPENDENCIES)
            {
                throw std::invalid_argument("fabric revision has too many dependencies");
            }
            if (revision.self_predecessor.has_value())
            {
                require_positive(*revision.self_predecessor, "self predecessor");
                if (*revision.self_predecessor >= revision.output_version)
                {
                    throw std::invalid_argument(
                        "fabric self predecessor must precede the output version");
                }
            }
            if (revision.as_of <= MIN_DT)
            {
                throw std::invalid_argument("fabric revision as_of must be a real instant");
            }
        }

        void validate_revision_dependencies(const DataRevisionInput &revision)
        {
            for (std::size_t index = 0; index < revision.dependencies.size(); ++index)
            {
                const auto &dependency = revision.dependencies[index];
                require_data_id(dependency.data_id);
                require_positive(dependency.version, "dependency version");
                if (dependency.data_id == revision.data_id)
                {
                    throw std::invalid_argument(
                        "fabric dependencies must not contain the output data id");
                }
                if (index != 0 && revision.dependencies[index - 1].data_id == dependency.data_id)
                {
                    throw std::invalid_argument("fabric dependency ids must be unique");
                }
            }
        }
    }  // namespace

    detail::FabricMetadataValueBinding::FabricMetadataValueBinding()
    {
        auto &factory = ValuePlanFactory::instance();
        int_type_ = factory.type_for(scalar_descriptor<Int>::value_meta());
        str_type_ = factory.type_for(scalar_descriptor<Str>::value_meta());
        datetime_type_ = factory.type_for(scalar_descriptor<DateTime>::value_meta());
        dependency_type_ =
            factory.type_for(scalar_descriptor<DataDependency>::value_meta());
        const auto *dependencies_meta =
            scalar_descriptor<HomogeneousTuple<DataDependency>>::value_meta();
        dependencies_type_ =
            compact_list_type(dependency_type_, *dependencies_meta);
        revision_type_ = factory.type_for(scalar_descriptor<DataRevision>::value_meta());
        reference_type_ =
            factory.type_for(scalar_descriptor<RevisionReference>::value_meta());
        if (!int_type_ || !str_type_ || !datetime_type_ || !dependency_type_ ||
            !dependencies_type_ || !revision_type_ || !reference_type_)
        {
            throw std::logic_error("fabric metadata value bindings did not resolve");
        }
    }

    Value detail::FabricMetadataValueBinding::make_data_dependency(
        DataDependencyInput dependency) const
    {
        require_data_id(dependency.data_id);
        require_positive(dependency.version, "dependency version");
        BundleBuilder builder{dependency_type_};
        builder.set(0U, atomic(str_type_, dependency.data_id));
        builder.set(1U, atomic(int_type_, dependency.version));
        return builder.build();
    }

    Value detail::FabricMetadataValueBinding::make_data_revision(
        DataRevisionInput revision) const
    {
        validate_revision_header(revision);

        std::ranges::sort(revision.dependencies,
                          [](const DataDependencyInput &lhs,
                             const DataDependencyInput &rhs) {
                              return canonical_data_id_less(lhs.data_id,
                                                            rhs.data_id);
                          });
        validate_revision_dependencies(revision);

        ListBuilder dependencies{dependency_type_};
        for (auto &dependency : revision.dependencies)
        {
            dependencies.push_back(make_data_dependency(std::move(dependency)));
        }
        ListStorage dependency_storage = dependencies.build_storage();
        Value dependency_value{dependencies_type_, &dependency_storage};

        BundleBuilder builder{revision_type_};
        builder.set(0U, atomic(int_type_, revision.format_version));
        builder.set(1U, atomic(str_type_, revision.data_id));
        builder.set(2U, atomic(int_type_, revision.revision));
        builder.set(3U, atomic(int_type_, revision.output_version));
        builder.set(4U, std::move(dependency_value));
        if (revision.self_predecessor.has_value())
        {
            builder.set(5U, atomic(int_type_, *revision.self_predecessor));
        }
        builder.set(6U, atomic(datetime_type_, revision.as_of));
        return builder.build();
    }

    DataRevisionInput detail::FabricMetadataValueBinding::data_revision_input(
        ValueView revision) const
    {
        if (!revision.valid() || revision.schema() != revision_type_.schema())
        {
            throw std::invalid_argument("expected a fabric DataRevision value");
        }
        const auto fields = revision.as_bundle();
        DataRevisionInput result{
            .format_version = fields.at(0U).checked_as<Int>(),
            .data_id = fields.at(1U).checked_as<Str>(),
            .revision = fields.at(2U).checked_as<Int>(),
            .output_version = fields.at(3U).checked_as<Int>(),
            .dependencies = {},
            .self_predecessor = {},
            .as_of = fields.at(6U).checked_as<DateTime>(),
        };
        const ValueView predecessor = fields.at(5U);
        if (predecessor.valid())
        {
            result.self_predecessor = predecessor.checked_as<Int>();
        }
        for (const auto dependency : fields.at(4U).as_list())
        {
            const auto dependency_fields = dependency.as_bundle();
            result.dependencies.push_back(DataDependencyInput{
                .data_id = dependency_fields.at(0U).checked_as<Str>(),
                .version = dependency_fields.at(1U).checked_as<Int>(),
            });
        }

        if (!std::ranges::is_sorted(
                result.dependencies,
                [](const DataDependencyInput &lhs,
                   const DataDependencyInput &rhs) {
                    return canonical_data_id_less(lhs.data_id, rhs.data_id);
                }))
        {
            throw std::invalid_argument(
                "fabric revision dependencies are not in canonical order");
        }
        validate_revision_header(result);
        validate_revision_dependencies(result);
        return result;
    }

    Value detail::FabricMetadataValueBinding::make_revision_reference(
        MetadataObjectKind kind, RevisionId revision) const
    {
        require_positive(revision, "revision reference");
        const Str kind_value{reference_kind_name(kind)};
        BundleBuilder builder{reference_type_};
        builder.set(0U, atomic(str_type_, kind_value));
        builder.set(1U, atomic(int_type_, revision));
        return builder.build();
    }

    RevisionId detail::FabricMetadataValueBinding::revision_reference_id(
        ValueView reference, MetadataObjectKind expected_kind) const
    {
        if (!reference.valid() || reference.schema() != reference_type_.schema())
        {
            throw std::invalid_argument(
                "expected a fabric RevisionReference value");
        }
        const auto fields = reference.as_bundle();
        const auto kind = fields.at(0U);
        const auto revision = fields.at(1U);
        if (!kind.valid() || !revision.valid())
        {
            throw std::invalid_argument("fabric revision reference is incomplete");
        }
        const auto expected_name = reference_kind_name(expected_kind);
        const auto stored_kind = kind.checked_as<Str>();
        if (stored_kind != expected_name)
        {
            throw std::invalid_argument("fabric revision reference is a '" +
                                        std::string{stored_kind} + "' entry, expected '" +
                                        std::string{expected_name} + "'");
        }
        const auto stored_revision = revision.checked_as<Int>();
        require_positive(stored_revision, "revision reference");
        return stored_revision;
    }

    const ValueTypeMetaData *
    detail::FabricMetadataValueBinding::data_revision_schema() const noexcept
    {
        return revision_type_.schema();
    }

    const ValueTypeMetaData *
    detail::FabricMetadataValueBinding::revision_reference_schema() const noexcept
    {
        return reference_type_.schema();
    }

    Value make_data_dependency(DataDependencyInput dependency)
    {
        return detail::FabricMetadataValueBinding{}.make_data_dependency(
            std::move(dependency));
    }

    Value make_data_revision(DataRevisionInput revision)
    {
        return detail::FabricMetadataValueBinding{}.make_data_revision(
            std::move(revision));
    }

    DataRevisionInput data_revision_input(ValueView revision)
    {
        return detail::FabricMetadataValueBinding{}.data_revision_input(
            std::move(revision));
    }
}  // namespace hgraph::fabric
