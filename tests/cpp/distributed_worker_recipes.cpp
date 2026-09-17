#include "distributed_worker_recipes.h"

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/distributed_map.h>
#include <hgraph/types/metadata/type_registry.h>

namespace hgraph_test
{
    void register_distributed_test_recipes()
    {
        hgraph::distributed::register_distributed_map_worker<RunningTotalG, Int, Int, Int>();
        hgraph::distributed::register_distributed_map_worker<DelayedDoubleG, Int, Int, Int>();
        hgraph::distributed::register_distributed_map_worker<ArmSilentlyG, Int, Int, Int>();

        // The same child as RunningTotalG, under a name chosen to be awkward
        // to pass to a process rather than derived from a type.
        using namespace hgraph::distributed;
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
