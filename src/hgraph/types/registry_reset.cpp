#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/temporal.h>

#include <hgraph/runtime/executor.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/types/value/value_conversion.h>

namespace hgraph
{
    void reset_all_registries() noexcept
    {
        // Ordering contract: borrowers before lenders. See the header for the
        // full rationale; the type registry must be LAST because it owns the
        // schemas every other registry borrows by pointer.
        // Construct TypeRegistry before resetting any registry: instance()
        // seeds the standard vocabulary on first touch and future seeding may
        // create common type records. Values now retain their TypeRecord as the
        // storage identity, so registries owning Values must be cleared before
        // the common records. The records are still cleared before their plan,
        // ops, and schema lenders below. If TypeRegistry's first touch
        // happened at its reset() call at the END of this function, the clears
        // would run BEFORE the first seed instead of between the seeds, leaving
        // prior-generation entries to collide with re-seeded metas on reused
        // addresses (the stale-pointer-reuse class; conflict manifests as
        // "already registered with a different plan").
        TypeRegistry &type_registry = TypeRegistry::instance();

        OperatorRegistry::instance().reset();
        ValueConversionRegistry::instance().reset();
        clear_json_converters();   // interns by meta/binding pointer — must precede the lenders below
        clear_table_converters();  // same rule (also captures record_replay config keys)
        clear_time_zone_provider_cache();  // borrows ZoneId handles; withdraw before names
        clear_zone_name_registry();  // invalidates process-local ZoneId handles between tests
        // Records borrow plan/ops contexts from the endpoint and TSData
        // factories. Their cached handles are trivial and are not dereferenced
        // while those factories are subsequently cleared.
        TypeRecordRegistry::instance().reset();
        clear_type_realization_snapshots();
        clear_executor_runtime_types();
        clear_graph_runtime_types();
        clear_node_runtime_types();
        TSInputBuilderFactory::reset();
        TSDataPlanFactory::instance().reset();
        ValuePlanFactory::instance().reset();
        clear_compact_container_plans();
        clear_mutable_container_plans();
        MemoryUtils::clear_synthesised_plans();
        type_registry.reset();  // re-seeds the standard scalar/TS vocabulary
    }
}  // namespace hgraph
