#include <hgraph/fabric/planning.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value_builder.h>

#include <algorithm>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace hgraph::fabric
{
    Str dependency_plan_trait(std::string_view path)
    {
        if (path.empty())
        {
            throw std::invalid_argument("fabric service path must not be empty");
        }
        if (path == "fabric")
        {
            return Str{DEPENDENCY_PLAN_TRAIT};
        }
        Str result{DEPENDENCY_PLAN_TRAIT};
        result.push_back('/');
        result.append(path);
        return result;
    }

    namespace
    {
        template <typename Schema>
        [[nodiscard]] Value bundle(
            std::vector<std::pair<std::string_view, Value>> fields)
        {
            BundleBuilder builder{ValuePlanFactory::instance().type_for(
                scalar_descriptor<Schema>::value_meta())};
            for (auto &[name, field] : fields)
            {
                builder.set(name, std::move(field));
            }
            return builder.build();
        }

        template <typename Element>
        [[nodiscard]] Value list(std::vector<Value> values)
        {
            const auto element = ValuePlanFactory::instance().type_for(
                scalar_descriptor<Element>::value_meta());
            const auto tuple = ValuePlanFactory::instance().type_for(
                scalar_descriptor<HomogeneousTuple<Element>>::value_meta());
            if (!element || !tuple)
            {
                throw std::logic_error(
                    "fabric dependency-plan tuple schema did not resolve");
            }
            ListBuilder builder{element};
            for (auto &value : values)
            {
                builder.push_back_copy(value.view().data());
            }
            ListStorage storage = builder.build_storage();
            return Value{tuple, &storage};
        }

        [[nodiscard]] Value strings(const std::vector<Str> &values)
        {
            std::vector<Value> materialised;
            materialised.reserve(values.size());
            for (const auto &value : values) { materialised.emplace_back(value); }
            return list<Str>(std::move(materialised));
        }

        void canonicalise_ids(std::vector<Str> &ids, std::string_view subject,
                              bool allow_empty)
        {
            if (!allow_empty && ids.empty())
            {
                throw std::invalid_argument("fabric " + std::string{subject} +
                                            " must not be empty");
            }
            for (const auto &id : ids) { require_data_id(id); }
            std::ranges::sort(ids, canonical_data_id_less);
            if (std::ranges::adjacent_find(ids) != ids.end())
            {
                throw std::invalid_argument("fabric " + std::string{subject} +
                                            " data ids must be unique");
            }
        }

        [[nodiscard]] bool contains(const std::vector<Str> &ids,
                                    std::string_view id)
        {
            return std::ranges::binary_search(ids, id, canonical_data_id_less);
        }

        void canonicalise(DependencyPlanInput &plan)
        {
            canonicalise_ids(plan.roots, "dependency-plan roots", true);

            for (auto &publisher : plan.publishers)
            {
                require_data_id(publisher.data_id);
                canonicalise_ids(publisher.dependencies,
                                 "publisher dependencies", true);
                if (contains(publisher.dependencies, publisher.data_id))
                {
                    throw std::invalid_argument(
                        "fabric publisher must not depend on its own data id");
                }
                for (const auto &dependency : publisher.dependencies)
                {
                    if (!contains(plan.roots, dependency))
                    {
                        throw std::invalid_argument(
                            "fabric publisher dependency is not a plan root");
                    }
                }
            }
            std::ranges::sort(
                plan.publishers,
                [](const PlannedPublisherInput &lhs,
                   const PlannedPublisherInput &rhs) {
                    return canonical_data_id_less(lhs.data_id, rhs.data_id);
                });
            if (std::ranges::adjacent_find(
                    plan.publishers,
                    [](const PlannedPublisherInput &lhs,
                       const PlannedPublisherInput &rhs) {
                        return lhs.data_id == rhs.data_id;
                    }) != plan.publishers.end())
            {
                throw std::invalid_argument(
                    "fabric dependency-plan publisher ids must be unique");
            }

            std::vector<Str> forest_roots;
            for (auto &forest : plan.forests)
            {
                canonicalise_ids(forest.roots, "consistency forest", false);
                forest_roots.insert(forest_roots.end(), forest.roots.begin(),
                                    forest.roots.end());
            }
            std::ranges::sort(
                plan.forests,
                [](const ConsistencyForestInput &lhs,
                   const ConsistencyForestInput &rhs) {
                    return canonical_data_id_less(lhs.roots.front(),
                                                  rhs.roots.front());
                });
            canonicalise_ids(forest_roots, "consistency-forest partition", true);
            if (forest_roots != plan.roots)
            {
                throw std::invalid_argument(
                    "fabric consistency forests must partition plan roots");
            }
        }

        [[nodiscard]] Value publisher_value(
            const PlannedPublisherInput &publisher)
        {
            return bundle<PlannedPublisher>({
                {"data_id", Value{publisher.data_id}},
                {"dependencies", strings(publisher.dependencies)},
            });
        }

        [[nodiscard]] Value forest_value(
            const ConsistencyForestInput &forest)
        {
            return bundle<ConsistencyForest>({{"roots", strings(forest.roots)}});
        }

        template <typename T>
        [[nodiscard]] std::vector<Str> read_strings(const T &value)
        {
            std::vector<Str> result;
            for (const auto item : value.as_list())
            {
                result.push_back(item.template checked_as<Str>());
            }
            return result;
        }
    }  // namespace

    Value make_dependency_plan(DependencyPlanInput plan)
    {
        register_fabric_types();
        canonicalise(plan);

        std::vector<Value> publishers;
        publishers.reserve(plan.publishers.size());
        for (const auto &publisher : plan.publishers)
        {
            publishers.push_back(publisher_value(publisher));
        }
        std::vector<Value> forests;
        forests.reserve(plan.forests.size());
        for (const auto &forest : plan.forests)
        {
            forests.push_back(forest_value(forest));
        }
        return bundle<DependencyPlan>({
            {"roots", strings(plan.roots)},
            {"publishers", list<PlannedPublisher>(std::move(publishers))},
            {"forests", list<ConsistencyForest>(std::move(forests))},
        });
    }

    DependencyPlanInput dependency_plan_input(ValueView plan)
    {
        register_fabric_types();
        if (!plan.valid() ||
            plan.schema() != scalar_descriptor<DependencyPlan>::value_meta())
        {
            throw std::invalid_argument(
                "expected a fabric DependencyPlan value");
        }

        const auto fields = plan.as_bundle();
        DependencyPlanInput result;
        result.roots = read_strings(fields.at("roots"));
        for (const auto item : fields.at("publishers").as_list())
        {
            const auto publisher = item.as_bundle();
            result.publishers.push_back(PlannedPublisherInput{
                .data_id = publisher.at("data_id").checked_as<Str>(),
                .dependencies = read_strings(publisher.at("dependencies")),
            });
        }
        for (const auto item : fields.at("forests").as_list())
        {
            const auto forest = item.as_bundle();
            result.forests.push_back(ConsistencyForestInput{
                .roots = read_strings(forest.at("roots")),
            });
        }

        DependencyPlanInput canonical = result;
        canonicalise(canonical);
        if (canonical != result)
        {
            throw std::invalid_argument(
                "fabric dependency plan is not canonical");
        }
        return result;
    }
}  // namespace hgraph::fabric
