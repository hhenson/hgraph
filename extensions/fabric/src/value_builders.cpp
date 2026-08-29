#include <hgraph/fabric/value_builders.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value_builder.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace hgraph::fabric
{
    namespace
    {
        template <typename T>
        [[nodiscard]] Value atomic(T value)
        {
            static_cast<void>(scalar_descriptor<T>::value_meta());
            return Value{std::move(value)};
        }

        template <typename Schema>
        [[nodiscard]] Value bundle(
            std::vector<std::pair<std::string_view, Value>> fields)
        {
            BundleBuilder builder{ValuePlanFactory::instance().type_for(
                scalar_descriptor<Schema>::value_meta())};
            for (auto &[name, field] : fields)
            {
                if (field.has_value()) { builder.set(name, std::move(field)); }
            }
            return builder.build();
        }

        [[nodiscard]] Value dependencies_value(
            std::vector<DataDependencyInput> dependencies)
        {
            const auto element = ValuePlanFactory::instance().type_for(
                scalar_descriptor<DataDependency>::value_meta());
            const auto tuple = ValuePlanFactory::instance().type_for(
                scalar_descriptor<HomogeneousTuple<DataDependency>>::value_meta());
            if (!element || !tuple)
            {
                throw std::logic_error("fabric dependency tuple schema did not resolve");
            }

            ListBuilder builder{element};
            for (auto &dependency : dependencies)
            {
                Value value = make_data_dependency(std::move(dependency));
                builder.push_back_copy(value.view().data());
            }
            ListStorage storage = builder.build_storage();
            return Value{tuple, &storage};
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

    Value make_data_dependency(DataDependencyInput dependency)
    {
        register_fabric_types();
        require_data_id(dependency.data_id);
        require_positive(dependency.version, "dependency version");
        return bundle<DataDependency>({
            {"data_id", atomic(std::move(dependency.data_id))},
            {"version", atomic(dependency.version)},
        });
    }

    Value make_data_revision(DataRevisionInput revision)
    {
        register_fabric_types();
        validate_revision_header(revision);

        std::ranges::sort(revision.dependencies,
                          [](const DataDependencyInput &lhs,
                             const DataDependencyInput &rhs) {
                              return canonical_data_id_less(lhs.data_id,
                                                            rhs.data_id);
                          });
        validate_revision_dependencies(revision);

        std::vector<std::pair<std::string_view, Value>> fields{
            {"format_version", atomic(revision.format_version)},
            {"data_id", atomic(std::move(revision.data_id))},
            {"revision", atomic(revision.revision)},
            {"output_version", atomic(revision.output_version)},
            {"dependencies", dependencies_value(std::move(revision.dependencies))},
            {"as_of", atomic(revision.as_of)},
        };
        if (revision.self_predecessor.has_value())
        {
            fields.emplace_back("self_predecessor",
                                atomic(*revision.self_predecessor));
        }
        return bundle<DataRevision>(std::move(fields));
    }

    DataRevisionInput data_revision_input(ValueView revision)
    {
        register_fabric_types();
        if (!revision.valid() ||
            revision.schema() != scalar_descriptor<DataRevision>::value_meta())
        {
            throw std::invalid_argument("expected a fabric DataRevision value");
        }
        const auto fields = revision.as_bundle();
        DataRevisionInput result{
            .format_version = fields.at("format_version").checked_as<Int>(),
            .data_id = fields.at("data_id").checked_as<Str>(),
            .revision = fields.at("revision").checked_as<Int>(),
            .output_version = fields.at("output_version").checked_as<Int>(),
            .dependencies = {},
            .self_predecessor = {},
            .as_of = fields.at("as_of").checked_as<DateTime>(),
        };
        const ValueView predecessor = fields.at("self_predecessor");
        if (predecessor.valid())
        {
            result.self_predecessor = predecessor.checked_as<Int>();
        }
        for (const auto dependency : fields.at("dependencies").as_list())
        {
            const auto dependency_fields = dependency.as_bundle();
            result.dependencies.push_back(DataDependencyInput{
                .data_id = dependency_fields.at("data_id").checked_as<Str>(),
                .version = dependency_fields.at("version").checked_as<Int>(),
            });
        }

        // Reject rather than silently canonicalise an already materialized
        // non-canonical value. That keeps the codec single-valued without
        // rebuilding the potentially large immutable revision.
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
}  // namespace hgraph::fabric
