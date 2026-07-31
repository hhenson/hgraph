#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_meta_data.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/type_pointer.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <bit>
#include <cstddef>
#include <type_traits>

namespace
{
    template <typename View>
    concept HasNoArgumentRemovedValue = requires(const View &view) {
        view.has_removed_value();
        view.removed_value();
    };
}  // namespace

TEST_CASE("current type-erasure records retain their baseline layouts")
{
    using namespace hgraph;

    static_assert(sizeof(ValueTypeRef) == sizeof(void *));
    static_assert(alignof(ValueTypeRef) == alignof(void *));
    static_assert(std::is_standard_layout_v<ValueTypeRef>);
    static_assert(std::is_trivially_copyable_v<ValueTypeRef>);
    static_assert(sizeof(NodeTypeRef) == sizeof(void *));
    static_assert(std::is_trivially_copyable_v<NodeTypeRef>);
    static_assert(sizeof(GraphTypeRef) == sizeof(void *));
    static_assert(std::is_trivially_copyable_v<GraphTypeRef>);
    static_assert(sizeof(ExecutorTypeRef) == sizeof(void *));
    static_assert(std::is_trivially_copyable_v<ExecutorTypeRef>);
    static_assert(sizeof(ClockTypeRef) == sizeof(void *));
    static_assert(std::is_trivially_copyable_v<ClockTypeRef>);
    static_assert(sizeof(ValueView) == sizeof(void *) * 2);
    static_assert(sizeof(Value) == sizeof(void *) * 3);
    static_assert(sizeof(AnyPtr) == sizeof(void *) * 2);
    static_assert(sizeof(TypedPtr<TypeFamily::Value>) == sizeof(void *) * 2);
    static_assert(sizeof(NodePtr) == sizeof(void *) * 2);
    static_assert(sizeof(GraphPtr) == sizeof(void *) * 2);
    static_assert(sizeof(ExecutorPtr) == sizeof(void *) * 2);
    static_assert(sizeof(ClockPtr) == sizeof(void *) * 2);
    static_assert(sizeof(NodeView) == sizeof(void *) * 2);
    static_assert(sizeof(GraphView) == sizeof(void *) * 2);
    static_assert(sizeof(GraphExecutorView) == sizeof(void *) * 2);
    static_assert(sizeof(EvaluationClockView) == sizeof(void *) * 2);
    static_assert(sizeof(NodeValue) == sizeof(void *) * 3);
    static_assert(sizeof(GraphValue) == sizeof(void *) * 5);
    static_assert(sizeof(GraphExecutorValue) == sizeof(void *) * 3);
    static_assert(GraphValue::debug_pointer_offset() == 0);

    // Every borrowed TSData cursor is the common two-word type/data pointer.
    static_assert(sizeof(TSDataStorageRef<>) == sizeof(void *) * 2);
    static_assert(sizeof(IndexedTSDataStorageRef) == sizeof(void *) * 2);
    static_assert(sizeof(TSSDataStorageRef) == sizeof(void *) * 2);
    static_assert(sizeof(TSDDataStorageRef) == sizeof(void *) * 2);
    static_assert(sizeof(TSWDataStorageRef) == sizeof(void *) * 2);
    static_assert(!HasNoArgumentRemovedValue<TSWDataView>);
    static_assert(HasNoArgumentRemovedValue<TSWInputView>);
    static_assert(sizeof(TSDataView) == sizeof(void *) * 2);
    static_assert(TS_DATA_OPS_ABI_VERSION == 5);
    static_assert(sizeof(TSRoleTypeRef) == sizeof(void *));
    static_assert(sizeof(TSDataObserverSet) == sizeof(void *));
    static_assert(sizeof(TSData) == sizeof(void *) * 3);
    static_assert(sizeof(TSParentLink) == sizeof(void *) * 3);
    static_assert(sizeof(TSDataTracking) == sizeof(void *) * 5);
    static_assert(sizeof(TimeSeriesReference) == sizeof(void *) * 5);
#if defined(__APPLE__) && defined(__aarch64__)
    // 272 -> 280: heterogeneous realized keys store the value binding
    // (m_value_binding) so keys hash/compare through the bound ValueOps.
    static_assert(sizeof(KeySlotStore) <= 280);
    static_assert(sizeof(KeyMirroredValueSlotStore) <= 208);
    static_assert(sizeof(TSDProxySlotSync) <= 24);
    static_assert(sizeof(TSDProxy) <= 400);
#endif
    static_assert(sizeof(TSOutputHandle) == sizeof(void *) * 3);
    static_assert(sizeof(TSOutputView) == sizeof(void *) * 4);
    // 8 -> 9 words (RFC 0008 stage 5 amendment): the input cursor carries a
    // non-owning prepared-route pointer (null outside the prepared path).
    static_assert(sizeof(TSInputView) == sizeof(void *) * 9);
    static_assert(sizeof(IndexedTSDataView) == sizeof(void *) * 2);
    static_assert(sizeof(TSBDataView) == sizeof(void *) * 2);
    static_assert(sizeof(TSLDataView) == sizeof(void *) * 2);
    static_assert(sizeof(TSBOutputView) == sizeof(void *) * 4);
    static_assert(sizeof(TSLOutputView) == sizeof(void *) * 4);
    static_assert(sizeof(TSBInputView) == sizeof(void *) * 9);
    static_assert(sizeof(TSLInputView) == sizeof(void *) * 9);
    static_assert(sizeof(TSOutput) == sizeof(void *) * 5);
    static_assert(sizeof(TSInput) == sizeof(void *) * 7);
    static_assert(sizeof(FixedTSDataFieldLayout) == sizeof(void *) * 3);
    static_assert(sizeof(FixedTSBDataLayout) == sizeof(void *) * 7);
    static_assert(sizeof(FixedTSLDataLayout) == sizeof(void *) * 10);

    using RawHandle = MemoryUtils::ErasedOwner<>;
    static_assert(sizeof(RawHandle) == sizeof(void *) * 3);
    static_assert(alignof(RawHandle) == alignof(void *));

    SUCCEED("compile-time type-erasure layout assertions passed");
}

TEST_CASE("dynamic TSL and TSW physical plans retain their baseline layouts")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    auto       &factory  = TSDataPlanFactory::instance();
    const auto *integer  = registry.register_scalar<std::int32_t>("type_erasure_layout_int32");
    const auto *ts       = registry.ts(integer);

    const auto *dynamic_schema = registry.tsl(ts, 0);
    const auto *tick_schema = registry.tsw(integer, 3, 1);
    const auto *duration_schema = registry.tsw_duration(integer, TimeDelta{3}, TimeDelta{1});
    const std::array schemas{dynamic_schema, tick_schema, duration_schema};

    for (const auto *schema : schemas)
    {
        const auto data = factory.data_type_for(schema);
        const auto output = factory.output_type_for(schema);
        TSInput owned{TSInputBuilderFactory::checked_builder_for(
            *schema, TSEndpointSchema::owned(schema))};
        const auto input = owned.type_ref();
        const auto &plan = data.checked_plan();

        REQUIRE(data.plan() == output.plan());
        REQUIRE(data.plan() == input.plan());
        REQUIRE(plan.layout.valid());
        REQUIRE(plan.layout.size > 0);
        REQUIRE(std::has_single_bit(plan.layout.alignment));
        REQUIRE(plan.layout.size % plan.layout.alignment == 0);

        const std::array role_types{data.as_role(), input.as_role(), output.as_role()};
        for (const auto role_type : role_types)
        {
            const auto &ops = role_type.ops_ref();
            const auto *common = ops.layout_impl(ops.context);
            REQUIRE(common != nullptr);
            REQUIRE(common->value_binding);
            REQUIRE(common->delta_binding);
            REQUIRE(common->value_offset < plan.layout.size);
            REQUIRE(common->tracking_offset < plan.layout.size);

            if (schema->kind == TSTypeKind::TSL)
            {
                const auto &layout = static_cast<const FixedTSLDataLayout &>(*common);
                REQUIRE(layout.element_type.record() != nullptr);
                REQUIRE(layout.element_type.role() == role_type.role());
                REQUIRE(layout.element_layout != nullptr);
                REQUIRE(layout.element_count == 0);
            }
            else
            {
                const auto &layout = static_cast<const TSWDataLayout &>(*common);
                REQUIRE(layout.element_binding);
                REQUIRE(layout.time_binding);
                if (schema->is_duration_based())
                {
                    const auto &duration = static_cast<const TimeTSWDataLayout &>(layout);
                    REQUIRE(duration.time_range == TimeDelta{3});
                    REQUIRE(duration.min_time_range == TimeDelta{1});
                }
                else
                {
                    const auto &tick = static_cast<const SizeTSWDataLayout &>(layout);
                    REQUIRE(tick.period == 3);
                    REQUIRE(tick.min_period == 1);
                }
            }
        }
    }

#if defined(__APPLE__) && defined(__aarch64__)
    const auto &dynamic = factory.data_type_for(dynamic_schema).checked_plan();
    const auto &tick = factory.data_type_for(tick_schema).checked_plan();
    const auto &duration = factory.data_type_for(duration_schema).checked_plan();
    REQUIRE(dynamic.layout.size == 96);
    REQUIRE(dynamic.layout.alignment == 8);
    REQUIRE(tick.layout.size == 136);
    REQUIRE(tick.layout.alignment == 8);
    REQUIRE(duration.layout.size == 136);
    REQUIRE(duration.layout.alignment == 8);
#endif
}

TEST_CASE("value ops discriminator has a fixed byte ABI at offset zero")
{
    using namespace hgraph;

    static_assert(sizeof(ValueOpsKind) == 1);
    static_assert(offsetof(ValueOps, kind) == 0);
    static_assert(std::is_same_v<decltype(VALUE_OPS_ABI_VERSION), const std::uint16_t>);
    static_assert(VALUE_OPS_ABI_VERSION == 3);
    static_assert(static_cast<std::uint8_t>(ValueOpsKind::Invalid) == 0);
    static_assert(static_cast<std::uint8_t>(ValueOpsKind::Base) == 1);
    static_assert(static_cast<std::uint8_t>(ValueOpsKind::Indexed) == 2);
    static_assert(static_cast<std::uint8_t>(ValueOpsKind::List) == 3);
    static_assert(static_cast<std::uint8_t>(ValueOpsKind::MutableList) == 4);
    static_assert(static_cast<std::uint8_t>(ValueOpsKind::CyclicBuffer) == 5);
    static_assert(static_cast<std::uint8_t>(ValueOpsKind::Queue) == 6);
    static_assert(static_cast<std::uint8_t>(ValueOpsKind::Set) == 7);
    static_assert(static_cast<std::uint8_t>(ValueOpsKind::MutableSet) == 8);
    static_assert(static_cast<std::uint8_t>(ValueOpsKind::Map) == 9);
    static_assert(static_cast<std::uint8_t>(ValueOpsKind::MutableMap) == 10);

    SUCCEED("compile-time value ops ABI assertions passed");
}

TEST_CASE("value schemas expose the unified schema header as their standard-layout prefix")
{
    using namespace hgraph;

    static_assert(std::is_standard_layout_v<ValueTypeMetaData>);
    static_assert(offsetof(ValueTypeMetaData, header) == 0);
    static_assert(offsetof(ValueTypeMetaData, flags) == sizeof(SchemaHeader));
    static_assert(!std::is_base_of_v<TypeMetaData, ValueTypeMetaData>);
    static_assert(!std::is_convertible_v<const ValueTypeMetaData *, const TypeMetaData *>);

    static_assert(std::is_standard_layout_v<TSValueTypeMetaData>);
    static_assert(offsetof(TSValueTypeMetaData, header) == 0);
    static_assert(!std::is_base_of_v<TypeMetaData, TSValueTypeMetaData>);
    static_assert(!std::is_convertible_v<const TSValueTypeMetaData *, const TypeMetaData *>);
    static_assert(sizeof(TSRoleTypeRef) == sizeof(void *));
    static_assert(sizeof(TSDataTypeRef) == sizeof(void *));
    static_assert(sizeof(TSInputTypeRef) == sizeof(void *));
    static_assert(sizeof(TSOutputTypeRef) == sizeof(void *));
    static_assert(sizeof(TSDataPtr) == sizeof(void *) * 2);
    static_assert(sizeof(TSInputPtr) == sizeof(void *) * 2);
    static_assert(sizeof(TSOutputPtr) == sizeof(void *) * 2);

    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Atomic) == 0);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Tuple) == 1);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Bundle) == 2);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::List) == 3);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Set) == 4);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Map) == 5);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::CyclicBuffer) == 6);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Queue) == 7);
    static_assert(static_cast<std::uint8_t>(ValueTypeKind::Any) == 8);
    static_assert(static_cast<std::uint8_t>(TSTypeKind::TS) == 0);
    static_assert(static_cast<std::uint8_t>(TSTypeKind::TSS) == 1);
    static_assert(static_cast<std::uint8_t>(TSTypeKind::TSD) == 2);
    static_assert(static_cast<std::uint8_t>(TSTypeKind::TSL) == 3);
    static_assert(static_cast<std::uint8_t>(TSTypeKind::TSW) == 4);
    static_assert(static_cast<std::uint8_t>(TSTypeKind::TSB) == 5);
    static_assert(static_cast<std::uint8_t>(TSTypeKind::REF) == 6);
    static_assert(static_cast<std::uint8_t>(TSTypeKind::SIGNAL) == 7);

    const auto *meta = TypeRegistry::instance().value_type("int");
    REQUIRE(meta != nullptr);
    REQUIRE(&meta->schema_header() == reinterpret_cast<const SchemaHeader *>(meta));
    REQUIRE(meta->schema_header().valid());
    REQUIRE(meta->schema_header().family == TypeFamily::Value);
    REQUIRE(meta->schema_header().kind == static_cast<TypeKind>(meta->value_kind()));

    ValueTypeMetaData invalid{ValueTypeKind::Atomic, ValueTypeFlags::None, "invalid"};
    invalid.header.kind = static_cast<TypeKind>(ValueTypeKind::Any) + 1;
    REQUIRE_FALSE(invalid.try_value_kind().has_value());
    REQUIRE_FALSE(try_value_type_kind(invalid.header.kind).has_value());
    REQUIRE_THROWS_AS(invalid.value_kind(), std::invalid_argument);
}
