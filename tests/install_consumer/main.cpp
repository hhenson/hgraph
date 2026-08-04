#include <hgraph/lib/std/standard_types.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/runtime/registry_snapshot.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_record.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/storage_metrics.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/visitor.h>
#include <hgraph/types/type_pointer.h>
#include <hgraph/types/type_resolution.h>
#include <hgraph/types/utils/stable_slot_store.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/compound_scalar_storage.h>
#include <hgraph/types/value/polymorphic_value_type.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/visitor.h>

#include <arrow/api.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <type_traits>

namespace
{
    struct ConsumerExtensionScalar
    {
        std::int32_t value{0};
    };
}

int main()
{
    using namespace hgraph;

    static_assert(std::is_standard_layout_v<SchemaHeader>);
    static_assert(std::is_trivially_copyable_v<SchemaHeader>);
    static_assert(std::is_standard_layout_v<TypeRecord>);
    static_assert(std::is_trivially_copyable_v<TypeRecord>);
    static_assert(std::is_standard_layout_v<AnyPtr>);
    static_assert(std::is_trivially_copyable_v<AnyPtr>);
    static_assert(TS_DATA_OPS_ABI_VERSION == 5);
    static_assert(sizeof(CompoundScalarStorageView) == 2 * sizeof(void *));
    static_assert(std::is_trivially_copyable_v<CompoundScalarStorageView>);
    static_assert(sizeof(PolymorphicValueType) == 2 * sizeof(void *));
    static_assert(std::is_standard_layout_v<PolymorphicValueType>);

    CompoundScalarStorage compound_storage =
        CompoundScalarStorage::make_default();
    const auto compound_view = compound_storage.view();
    const auto compound_inspection = compound_view.inspect();
    if (!compound_view.available() || compound_inspection.leaf_pool_count != 0 ||
        compound_inspection.live_slot_count != 0 ||
        compound_inspection.slot_capacity != 0)
    {
        throw std::runtime_error(
            "installed compound-scalar storage contract is unusable");
    }

    PolymorphicValueType empty_polymorphic_type;
    if (empty_polymorphic_type.binding())
    {
        throw std::runtime_error(
            "installed polymorphic value type no-op contract is unusable");
    }

    GraphBuilder inline_graph;
    if (inline_graph.root_type().checked_plan().find_component(
            "compound_scalar_storage") != nullptr)
    {
        throw std::runtime_error(
            "default installed graph unexpectedly planned pooled storage");
    }
    GraphBuilder pooled_graph;
    set_pooled_compound_scalar_storage(pooled_graph.global_state());
    if (pooled_graph.root_type().checked_plan().find_component(
            "compound_scalar_storage") == nullptr)
    {
        throw std::runtime_error(
            "installed pooled graph configuration was not realized");
    }

    StableSlotStore<StableSlotStateModel::ConstructedAndLive> stable_slots{
        MemoryUtils::StorageLayout{sizeof(std::uintptr_t), alignof(std::uintptr_t)}};
    stable_slots.reserve_to(4);
    auto *stable_value = std::construct_at(
        MemoryUtils::cast<std::uintptr_t>(stable_slots.slot_memory(0)),
        std::uintptr_t{41});
    stable_slots.mark_staged(0);
    if (!stable_slots.mark_live(0) ||
        stable_slots.representation() != StableSlotRepresentation::TaggedPointer ||
        stable_slots.live_slot_memory(0) != stable_value)
    {
        throw std::runtime_error("installed stable-slot tagged strategy is unusable");
    }
    std::destroy_at(stable_value);
    stable_slots.mark_free(0);

    StableSlotStore<StableSlotStateModel::ConstructedOnly> weak_stable_slots{
        MemoryUtils::StorageLayout{3, 1}};
    weak_stable_slots.reserve_to(4);
    if (weak_stable_slots.representation() != StableSlotRepresentation::Bitmap)
    {
        throw std::runtime_error("installed stable-slot bitmap strategy is unusable");
    }

    using ConsumerScalar =
        Bundle<"consumer::Scalar", Field<"number", Int>, Field<"label", Str>>;
    using ConsumerBundle = TSBFromScalar<ConsumerScalar>;
    static_assert(std::is_same_v<ConsumerBundle,
                                 TSB<"consumer::Scalar", Field<"number", TS<Int>>,
                                     Field<"label", TS<Str>>>>);

    auto &registry = TypeRegistry::instance();
    registry.register_scalar<std::int32_t>("int32");

    const RuntimeRegistrySnapshot runtime_registries =
        runtime_registry_snapshot();
    if (runtime_registries.type_records == 0)
    {
        throw std::runtime_error(
            "installed runtime registry snapshot did not observe seeded types");
    }

    static const std::byte consumer_node_type_id{};
    NodeTypeDescriptor first_node_descriptor;
    first_node_descriptor.schema.display_name = "consumer_canonical_node";
    const NodeBuilder first_node = NodeBuilder::from_canonical_descriptor(
        std::move(first_node_descriptor), &consumer_node_type_id);
    NodeTypeDescriptor repeated_node_descriptor;
    repeated_node_descriptor.schema.display_name = "consumer_canonical_node";
    const NodeBuilder repeated_node = NodeBuilder::from_canonical_descriptor(
        std::move(repeated_node_descriptor), &consumer_node_type_id);
    if (repeated_node.type() != first_node.type()) {
      throw std::runtime_error(
          "installed canonical node descriptor did not reuse its runtime type");
    }

    Value value{std::int32_t{41}};
    auto view = value.view();
    const AnyPtr pointer = AnyPtr::read_only(*view.record(), view.data());
    if (!pointer.valid() || pointer.family() != TypeFamily::Value || !pointer.read_only_access())
    {
        throw std::runtime_error("installed target produced an invalid erased pointer");
    }

    value.begin_mutation().as<std::int32_t>() = 73;
    if (value.view().checked_as<std::int32_t>() != 73)
    {
        throw std::runtime_error("installed target mutation round-trip failed");
    }

    registry.register_scalar<ConsumerExtensionScalar>("consumer::ExtensionScalar");
    Value extension{ConsumerExtensionScalar{17}};
    Value boxed{any_type()};
    boxed.as_any().begin_mutation().set(extension.view());
    const bool visited_extension = visit(
        boxed.view(),
        [](AtomicView selected) {
            return selected.holds_alternative<ConsumerExtensionScalar>() &&
                   selected.checked_as<ConsumerExtensionScalar>().value == 17;
        },
        [](ValueView) { return false; });
    if (!visited_extension)
    {
        throw std::runtime_error("installed value visitor did not unwrap and dispatch the extension scalar");
    }

    static_cast<void>(stdlib::register_standard_types(registry));

    Value long_text{Str(256, 'x')};
    const DynamicStorageMetrics value_storage =
        long_text.view().dynamic_storage_metrics();
    if (value_storage.live_bytes < 257 ||
        value_storage.reserved_bytes < value_storage.live_bytes)
    {
        throw std::runtime_error("installed Value dynamic storage metrics are unusable");
    }

    const auto *ts_int = registry.ts(scalar_descriptor<Int>::value_meta());
    TSOutput    output{*ts_int};
    auto        output_view = output.view(MIN_ST);
    const bool  visited_value = visit(
        output_view,
        [](TSValueOutputView selected) { return selected.schema()->kind == TSTypeKind::TS; },
        [](TSOutputView) { return false; });
    if (!visited_value)
    {
        throw std::runtime_error("installed endpoint visitor dispatched the wrong time-series kind");
    }

    const auto *tss_int = registry.tss(scalar_descriptor<Int>::value_meta());
    TSOutput set_output{*tss_int};
    {
        auto set_data = set_output.data_view();
        auto set = set_data.as_set();
        auto mutation = set.begin_mutation(MIN_ST);
        mutation.reserve(16);
    }
    const DynamicStorageMetrics set_storage = set_output.data_view().dynamic_storage_metrics();
    if (set_storage.live_bytes == 0 || set_storage.reserved_bytes < set_storage.live_bytes)
    {
        throw std::runtime_error("installed TSData dynamic storage metrics are unusable");
    }

    using TypedFrame = FrameOf<Bundle<"consumer::Row", Field<"value", Int>>,
                               Bundle<"consumer::Metadata", Field<"revision", Int>>>;
    const auto *typed_frame = scalar_type<TypedFrame>();
    if (typed_frame == nullptr || typed_frame->key_type == nullptr)
    {
        throw std::runtime_error("installed typed frame metadata schema is unusable");
    }

    arrow::Int64Builder values;
    if (!values.Append(42).ok())
    {
        throw std::runtime_error("installed Arrow builder is unusable");
    }
    std::shared_ptr<arrow::Array> array;
    if (!values.Finish(&array).ok())
    {
        throw std::runtime_error("installed Arrow array construction failed");
    }
    Frame frame{arrow::Table::Make(
        arrow::schema({arrow::field("value", arrow::int64())}),
        {std::move(array)})};
    BundleBuilder metadata{ValuePlanFactory::instance().type_for(typed_frame->key_type)};
    metadata.set(0, Value{Int{7}});
    frame = with_frame_metadata(std::move(frame), metadata.build());
    if (!frame.has_metadata() ||
        frame_metadata(frame, typed_frame->key_type).as_bundle().at(0).checked_as<Int>() != 7)
    {
        throw std::runtime_error("installed Arrow frame metadata codec is unusable");
    }

    return 0;
}
