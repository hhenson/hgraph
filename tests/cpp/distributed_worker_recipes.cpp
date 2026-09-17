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
    }
}  // namespace hgraph_test
