// Ensures the foundation headers compile cleanly together.
// This test instantiates a few of the templated entry points so the
// compiler chases through the heavier portions of memory_utils, the
// slot stores, and intern_table.

#include <hgraph/manifest/canonical.h>
#include <hgraph/manifest/graph_manifest.h>
#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/type_meta_data.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/utils/intern_table.h>
#include <hgraph/types/utils/key_slot_store.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/utils/slot_observer.h>
#include <hgraph/types/utils/stable_slot_storage.h>
#include <hgraph/types/utils/stable_slot_store.h>
#include <hgraph/types/utils/value_slot_store.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/time_series/ts_data/dict_view.h>
#include <hgraph/types/time_series/ts_data/indexed_view.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/time_series/ts_data/set_view.h>
#include <hgraph/types/time_series/ts_data/storage.h>
#include <hgraph/types/time_series/ts_data/types.h>
#include <hgraph/types/time_series/ts_data/window_view.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/type_pointer.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/scope.h>
#include <hgraph/util/tagged_ptr.h>

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace {

void instantiate_memory_utils() {
    using hgraph::MemoryUtils;
    const auto &plan = MemoryUtils::plan_for<std::int32_t>();
    assert(plan.valid());
    assert(plan.layout.size == sizeof(std::int32_t));

    [[maybe_unused]] const auto &tuple_plan = MemoryUtils::tuple_plan({&plan, &plan});
    assert(tuple_plan.is_tuple());
    assert(tuple_plan.component_count() == 2);

    [[maybe_unused]] const auto &array_plan = MemoryUtils::array_plan(plan, 4);
    assert(array_plan.is_array());
    assert(array_plan.array_count() == 4);

    MemoryUtils::ErasedOwner<MemoryUtils::InlineStoragePolicy<>, void> owner(plan);
    assert(owner.has_value());
}

void instantiate_intern_table() {
    hgraph::InternTable<int, int> table;
    [[maybe_unused]] const int &v = table.emplace(7, 7);
    assert(v == 7);
}

void instantiate_type_pointer() {
    [[maybe_unused]] constexpr hgraph::AnyPtr any{};
    [[maybe_unused]] constexpr hgraph::NodePtr node{};
    static_assert(any.is_unbound());
    static_assert(node.is_unbound());
}

void instantiate_slot_stores() {
    using hgraph::MemoryUtils;
    using hgraph::KeySlotStore;
    using hgraph::KeyMirroredValueSlotStore;
    using hgraph::ValueSlotStore;

    hgraph::StableSlotStore<hgraph::StableSlotStateModel::ConstructedAndLive> stable_slots(
        MemoryUtils::layout_for<std::int64_t>());
    stable_slots.reserve_to(8);
    assert(stable_slots.representation() == hgraph::StableSlotRepresentation::TaggedPointer);

    KeySlotStore keys(MemoryUtils::plan_for<std::int32_t>(), hgraph::key_slot_store_ops_for<std::int32_t>());
    KeyMirroredValueSlotStore mirrored_values(keys, MemoryUtils::plan_for<std::int32_t>());
    int          k = 42;
    [[maybe_unused]] auto result = keys.insert(k);
    assert(result.inserted);
    assert(keys.contains(k));
    assert(mirrored_values.has_slot(result.slot));
    assert(mirrored_values.mirrors_key_construction());

    ValueSlotStore values(MemoryUtils::plan_for<std::int32_t>());
    values.reserve_to(8);
    values.construct_at<std::int32_t>(0, 13);
    assert(values.has_slot(0));
}

}  // namespace

void instantiate_schema() {
    using hgraph::TypeRegistry;
    auto       &registry = TypeRegistry::instance();
    [[maybe_unused]] const auto *standard_int_meta = registry.value_type("int");
    assert(standard_int_meta != nullptr);
    assert(standard_int_meta == registry.scalar_type<std::int64_t>().schema());

    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    assert(int_meta != nullptr);
    assert(int_meta->value_kind() == hgraph::ValueTypeKind::Atomic);
    assert(int_meta == registry.value_type("int32"));

    [[maybe_unused]] const auto *ts_int = registry.ts(int_meta);
    assert(ts_int != nullptr);
    assert(ts_int->kind == hgraph::TSTypeKind::TS);
}

void instantiate_plan_factory() {
    using hgraph::MemoryUtils;
    using hgraph::TypeRegistry;
    using hgraph::ValuePlanFactory;

    auto &registry = TypeRegistry::instance();
    auto &factory  = ValuePlanFactory::instance();

    const auto *int_meta   = registry.register_scalar<std::int32_t>("int32");
    const auto *float_meta = registry.register_scalar<float>("float32");

    [[maybe_unused]] const auto *int_plan   = factory.plan_for(int_meta);
    [[maybe_unused]] const auto *float_plan = factory.plan_for(float_meta);
    assert(int_plan == &MemoryUtils::plan_for<std::int32_t>());
    assert(float_plan == &MemoryUtils::plan_for<float>());

    const auto *tuple_meta = registry.tuple({int_meta, float_meta});
    [[maybe_unused]] const auto *tuple_plan = factory.plan_for(tuple_meta);
    assert(tuple_plan != nullptr);
    assert(tuple_plan->is_tuple());
    assert(tuple_plan->component_count() == 3);
    assert(tuple_plan->component(0).plan == int_plan);
    assert(tuple_plan->component(1).plan == float_plan);
    [[maybe_unused]] const auto *validity_plan = &MemoryUtils::array_plan<std::uint64_t>(1);
    assert(tuple_plan->component(2).plan == validity_plan);
    // Composite plans are interned by MemoryUtils — same triple yields same pointer.
    assert(tuple_plan == &MemoryUtils::tuple_plan({int_plan, float_plan, validity_plan}));

    // Caching: a second lookup returns the same pointer without re-synthesising.
    assert(factory.plan_for(tuple_meta) == tuple_plan);
    assert(factory.find(tuple_meta) == tuple_plan);
}

int main() {
    instantiate_memory_utils();
    instantiate_intern_table();
    instantiate_type_pointer();
    instantiate_slot_stores();
    instantiate_schema();
    instantiate_plan_factory();
    return 0;
}
