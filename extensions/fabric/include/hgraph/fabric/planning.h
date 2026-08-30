#ifndef HGRAPH_FABRIC_PLANNING_H
#define HGRAPH_FABRIC_PLANNING_H

#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <hgraph/types/value/value.h>

#include <string>
#include <string_view>
#include <vector>

namespace hgraph::fabric
{
    inline constexpr std::string_view DEPENDENCY_PLAN_TRAIT{
        "hgraph.fabric.dependency_plan"};

    /** Graph trait key for one Fabric service path. The default path retains
        DEPENDENCY_PLAN_TRAIT for source compatibility. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Str
    dependency_plan_trait(std::string_view path);

    using PlannedPublisher =
        Bundle<"hgraph.fabric::PlannedPublisher", Field<"data_id", Str>,
               Field<"dependencies", HomogeneousTuple<Str>>>;

    using ConsistencyForest =
        Bundle<"hgraph.fabric::ConsistencyForest",
               Field<"roots", HomogeneousTuple<Str>>>;

    using DependencyPlan =
        Bundle<"hgraph.fabric::DependencyPlan",
               Field<"roots", HomogeneousTuple<Str>>,
               Field<"publishers", HomogeneousTuple<PlannedPublisher>>,
               Field<"forests", HomogeneousTuple<ConsistencyForest>>>;

    struct PlannedPublisherInput
    {
        Str              data_id{};
        std::vector<Str> dependencies{};

        friend bool operator==(const PlannedPublisherInput &,
                               const PlannedPublisherInput &) = default;
    };

    struct ConsistencyForestInput
    {
        std::vector<Str> roots{};

        friend bool operator==(const ConsistencyForestInput &,
                               const ConsistencyForestInput &) = default;
    };

    /** Data-only result of RFC 0026 wiring-time dependency discovery.

        Roots, publisher dependencies, and forest members use canonical data-id
        order. Forests are ordered by their first root. The plan is attached to
        the compiled graph through DEPENDENCY_PLAN_TRAIT; no lineage work is
        added to ordinary node evaluation. */
    struct DependencyPlanInput
    {
        std::vector<Str>                    roots{};
        std::vector<PlannedPublisherInput>  publishers{};
        std::vector<ConsistencyForestInput> forests{};

        friend bool operator==(const DependencyPlanInput &,
                               const DependencyPlanInput &) = default;
    };

    /** Canonicalise, validate, and materialise a wiring dependency plan.
        Cost: O((V + E) log(V + E)) wiring-time work and O(V + E) storage. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Value
    make_dependency_plan(DependencyPlanInput plan);

    /** Extract and validate a canonical dependency plan value. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT DependencyPlanInput
    dependency_plan_input(ValueView plan);
}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_PLANNING_H
