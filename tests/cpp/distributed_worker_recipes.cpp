#include "distributed_worker_recipes.h"

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/distributed_map.h>
#include <hgraph/runtime/distributed_map_wiring.h>
#include <array>
#include <hgraph/types/metadata/type_registry.h>

#include <hgraph/util/environment.h>
#include <filesystem>
#include <fstream>
#include <random>

namespace hgraph_test
{
    void CheckpointStopMarker::stop()
    {
        // One file per stopped node, named so that neither two nodes of one
        // process nor two worker processes can collide.
        const auto directory = hgraph::environment_variable(stop_marker_directory_variable);
        if (!directory) { return; }
        std::random_device entropy;
        const auto name = std::to_string((static_cast<std::uint64_t>(entropy()) << 32) | entropy());
        std::ofstream{std::filesystem::path{*directory} / name} << "stopped";
    }

    void register_distributed_test_recipes()
    {
        hgraph::distributed::register_distributed_map_worker<RunningTotalG, Int, Int, Int>();
        hgraph::distributed::register_distributed_map_worker<DelayedDoubleG, Int, Int, Int>();
        hgraph::distributed::register_distributed_map_worker<ArmSilentlyG, Int, Int, Int>();
        hgraph::distributed::register_distributed_map_worker<AccumulateG, Int, Int, Int>();
        hgraph::distributed::register_distributed_map_worker<PreparedHostedChild, Int, Int, Int>();

        // The same child as RunningTotalG, under a name chosen to be awkward
        // to pass to a process rather than derived from a type.
        using namespace hgraph::distributed;
        register_prepared_worker_recipe(prepared_add_name, {+[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 3> inputs{{
                {schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()},
                {schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()},
                {schema_descriptor<TS<Int>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<PreparedAdd>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        }});
        register_prepared_worker_recipe(prepared_accumulate_name, {+[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<PreparedAccumulate>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        }});
        register_prepared_worker_recipe(prepared_nested_name, {+[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TSD<Str, TS<Int>>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<PreparedNestedOwners>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        }});
        const auto hosted = [](const char *recipe, auto build) { register_prepared_worker_recipe(recipe, {build}); };
        hosted(prepared_hosted_name, +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<PreparedHostedChild>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted(prepared_hosted_forgetful_name, +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<PreparedHostedThenForgetful>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted(prepared_hosted_constant_name, +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<PreparedHostedWithConstant>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted("hosted timer", +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<HostedWithTimer>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted("immediate sink", +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<ChildWithImmediateSink>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted("hosted nested", +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<HostedNestedComponent>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted("compatible before", +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<HostedCompatibleComponent<false>>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted("compatible after", +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<HostedCompatibleComponent<true>>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted("named worker", +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<HostedNamedComponent<"worker">>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted("named worker boundary", +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<HostedNamedComponent<"worker.boundary">>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted(accumulate_stop_marker_name, +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<AccumulateWithStopMarker>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted("refusing compute, stop marker", +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<RefusingComputeWithStopMarker>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        hosted("pending compute", +[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{{schema_descriptor<TSD<Str, TS<Int>>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<CheckpointPendingCompute>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        });
        register_prepared_worker_recipe(prepared_keys_name, {+[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{
                {schema_descriptor<TSS<Str>>::ts_meta(), WiringPortRef::ArgTag::None, "__keys__"}}};
            auto plan = prepare_distributed_map(fn<PreparedKeyOnly>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        }});
        register_prepared_worker_recipe(prepared_bundle_name, {+[](std::size_t group, std::size_t groups) {
            const std::array<DistributedMapInput, 1> inputs{{
                {schema_descriptor<TSD<Str, PreparedRow>>::ts_meta()}}};
            auto plan = prepare_distributed_map(fn<PreparedBundleIdentity>(), inputs, {}, group, groups);
            return PreparedWorkerPlan{std::move(plan.child), std::move(plan.slots)};
        }});

        register_worker_recipe(
            awkward_recipe_name,
            WorkerRecipe{+[]() -> hgraph::GraphBuilder {
                             return hgraph::build_graph<DistributedWorkerGraph<Int, Int, Int>>(
                                 fn<RunningTotalG>());
                         },
                         +[]() -> BoundarySlots {
                             return distributed_map_slots<Int, Int, Int>();
                         }});
    }
}  // namespace hgraph_test
