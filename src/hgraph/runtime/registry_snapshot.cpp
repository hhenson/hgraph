#include <hgraph/runtime/registry_snapshot.h>
#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/utils/counted_mutex.h>

#include "registry_snapshot_detail.h"

namespace hgraph
{
    RuntimeRegistrySnapshot runtime_registry_snapshot()
    {
        return RuntimeRegistrySnapshot{
            .node_runtime_types = detail::node_runtime_type_count(),
            .graph_programs = detail::graph_program_count(),
            .graph_runtime_types = detail::graph_runtime_type_count(),
            .executor_runtime_types = detail::executor_runtime_type_count(),
            .type_records = TypeRecordRegistry::instance().size(),
            .type_system_lock_acquisitions = type_system_lock_count(),
        };
    }
}  // namespace hgraph
