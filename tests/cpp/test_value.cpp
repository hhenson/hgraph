// Tests for the value-layer plumbing: ``ValueOps`` synthesis,
// ``ValueTypeRef`` interning, ``ErasedOwner`` SBO behaviour, and the
// owning ``Value`` + non-owning ``ValueView`` round-trip for atomic and
// structured value-layer kinds.

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/type_resolution.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/types/value/value_view.h>

#include <compare>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace
{
    struct NoValueOpsScalar
    {
        std::int32_t value{0};
    };

    struct UnregisteredScalar
    {
        std::int32_t value{0};
    };
}

TEST_CASE("fixed shaped arrays retain inline capacity with a logical extent")
{
    using namespace hgraph;

    const auto *meta = TypeRegistry::instance().array(
        scalar_descriptor<Int>::value_meta(), 4);
    Value partial{ValuePlanFactory::instance().type_for(meta)};
    auto output = partial.as_list().begin_mutation();
    output.resize(2);
    Value first{Int{1}};
    Value second{Int{2}};
    output.at(0).copy_from(first.view());
    output.at(1).copy_from(second.view());

    CHECK(partial.schema()->fixed_size == 4);
    CHECK(partial.as_list().size() == 2);
    CHECK(partial.as_list().at(0).checked_as<Int>() == 1);
    CHECK(partial.as_list().at(1).checked_as<Int>() == 2);

    Value copy{partial.binding()};
    copy.begin_mutation().copy_from(partial.view());
    CHECK(copy.equals(partial));
    CHECK(copy.as_list().size() == 2);

    auto resized = copy.as_list().begin_mutation();
    resized.resize(4);
    CHECK(resized.size() == 4);
    CHECK_THROWS_AS(resized.resize(5), std::out_of_range);
}

TEST_CASE("Value storage metrics expose owned string and bytes allocations", "[memory]")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("str");
    (void)registry.register_scalar<Bytes>("bytes");

    const std::string text(256, 'x');
    Value string_value{text};
    const auto string_metrics = string_value.view().dynamic_storage_metrics();
    CHECK(string_metrics.live_bytes == text.size() + 1);
    CHECK(string_metrics.reserved_bytes >= string_metrics.live_bytes);

    Value bytes_value{Bytes{text}};
    const auto bytes_metrics = bytes_value.view().dynamic_storage_metrics();
    CHECK(bytes_metrics.live_bytes == text.size() + 1);
    CHECK(bytes_metrics.reserved_bytes >= bytes_metrics.live_bytes);

    Value short_value{std::string{"small"}};
    CHECK(short_value.view().dynamic_storage_metrics().live_bytes == 0);
    CHECK(short_value.view().dynamic_storage_metrics().reserved_bytes == 0);
}

TEST_CASE("Value storage metrics recurse through fixed compounds and Owned", "[memory]")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    auto &factory = ValuePlanFactory::instance();
    const auto *str_meta = registry.register_scalar<std::string>("str");
    const std::string text(256, 'x');

    Value tuple{factory.type_for(registry.tuple({str_meta, str_meta}))};
    tuple.as_tuple().begin_mutation().at(0).checked_mutable_as<std::string>() = text;
    CHECK(tuple.view().dynamic_storage_metrics().live_bytes >= text.size() + 1);

    Value fixed_list{factory.type_for(registry.list(str_meta, 2))};
    fixed_list.begin_mutation().as_mutable_indexed().at(1).checked_mutable_as<std::string>() = text;
    CHECK(fixed_list.view().dynamic_storage_metrics().live_bytes >= text.size() + 1);

    const auto *bundle_schema = registry.bundle(
        "MemoryMetricsBundle", {{"label", str_meta}});
    Value bundle{factory.type_for(bundle_schema)};
    bundle.as_bundle().begin_mutation().at("label").checked_mutable_as<std::string>() = text;
    const auto bundle_metrics = bundle.view().dynamic_storage_metrics();
    CHECK(bundle_metrics.live_bytes >= text.size() + 1);

    Value owned{factory.type_for(registry.owned(bundle_schema))};
    owned.as_bundle().begin_mutation().at("label").checked_mutable_as<std::string>() = text;
    const auto owned_metrics = owned.view().dynamic_storage_metrics();
    CHECK(owned_metrics.live_bytes > bundle_metrics.live_bytes);
    CHECK(owned_metrics.reserved_bytes >= owned_metrics.live_bytes);
}

TEST_CASE("Value TypeRecords carry atomic and fixed-composite debug descriptors",
          "[type-erasure][debug-descriptor]")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *int_schema = registry.register_scalar<std::int32_t>("int32");
    const auto *bool_schema = registry.register_scalar<bool>("bool");
    const auto *string_schema = registry.register_scalar<std::string>("str");

    const ValueTypeRef int_type = ValuePlanFactory::instance().type_for(int_schema);
    const ValueTypeRef bool_type = ValuePlanFactory::instance().type_for(bool_schema);
    const ValueTypeRef string_type = ValuePlanFactory::instance().type_for(string_schema);

    REQUIRE(int_type.record()->debug != nullptr);
    REQUIRE(int_type.record()->debug->valid());
    REQUIRE(int_type.record()->debug->layout == DebugLayoutKind::Atomic);
    REQUIRE(int_type.record()->debug->atomic_kind == DebugAtomicKind::SignedInteger);
    REQUIRE(bool_type.record()->debug->atomic_kind == DebugAtomicKind::Boolean);
    REQUIRE(string_type.record()->debug->atomic_kind == DebugAtomicKind::String);

    const auto *bundle_schema = registry.bundle(
        "DebugDescriptorPair", {{"number", int_schema}, {"enabled", bool_schema}});
    const ValueTypeRef bundle_type = ValuePlanFactory::instance().type_for(bundle_schema);
    const DebugDescriptor *debug = bundle_type.record()->debug;
    REQUIRE(debug != nullptr);
    REQUIRE(debug->valid());
    REQUIRE(debug->layout == DebugLayoutKind::FixedComposite);
    REQUIRE(debug->atomic_kind == DebugAtomicKind::Opaque);
    REQUIRE(debug->field_count == 2);
    REQUIRE(debug->fields != nullptr);
    REQUIRE(has_flag(debug->flags, DebugDescriptorFlags::HasValidityBitmap));
    REQUIRE(debug->validity_word_size == sizeof(std::uint64_t));

    const auto components = bundle_type.plan()->components();
    REQUIRE(components.size() == 3);
    REQUIRE(std::string{debug->fields[0].name} == "number");
    REQUIRE(debug->fields[0].offset == components[0].offset);
    REQUIRE(debug->fields[0].type == int_type.record());
    REQUIRE(debug->fields[0].validity_bit == 0);
    REQUIRE(has_flag(debug->fields[0].flags, DebugFieldFlags::Optional));
    REQUIRE(std::string{debug->fields[1].name} == "enabled");
    REQUIRE(debug->fields[1].offset == components[1].offset);
    REQUIRE(debug->fields[1].type == bool_type.record());
    REQUIRE(debug->fields[1].validity_bit == 1);
    REQUIRE(debug->validity_offset == components[2].offset);
}

TEST_CASE("Value TypeRecords describe contiguous and stable-slot collections",
          "[type-erasure][debug-descriptor]")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *int_schema = registry.register_scalar<std::int32_t>("int32");
    const ValueTypeRef int_type = ValuePlanFactory::instance().type_for(int_schema);

    const ValueTypeRef fixed = ValuePlanFactory::instance().type_for(registry.list(int_schema, 3));
    REQUIRE(fixed.record()->debug->layout == DebugLayoutKind::Sequence);
    REQUIRE(fixed.record()->debug->element_type == int_type.record());
    REQUIRE(fixed.record()->debug->dynamic_layout->valid());
    REQUIRE(has_flag(fixed.record()->debug->dynamic_layout->flags, DebugDynamicFlags::SizeIsConstant));
    REQUIRE(fixed.record()->debug->dynamic_layout->size_constant == 3);

    const ValueTypeRef compact = ValuePlanFactory::instance().type_for(registry.list(int_schema));
    REQUIRE(compact.record()->debug->layout == DebugLayoutKind::Sequence);
    REQUIRE(compact.record()->debug->dynamic_layout->kind == DebugDynamicKind::Contiguous);
    REQUIRE(has_flag(compact.record()->debug->dynamic_layout->flags, DebugDynamicFlags::DataIsIndirect));

    const ValueTypeRef compact_set = ValuePlanFactory::instance().type_for(registry.set(int_schema));
    REQUIRE(compact_set.record()->debug->layout == DebugLayoutKind::Sequence);
    REQUIRE(compact_set.record()->debug->element_type == int_type.record());
    REQUIRE(compact_set.record()->debug->dynamic_layout->kind == DebugDynamicKind::Contiguous);

    const ValueTypeRef mutable_list = ValuePlanFactory::instance().type_for(registry.mutable_list(int_schema));
    REQUIRE(mutable_list.record()->debug->dynamic_layout->kind == DebugDynamicKind::StableSlots);
    REQUIRE(has_flag(mutable_list.record()->debug->dynamic_layout->flags,
                     DebugDynamicFlags::DataIsPointerTable));

    const ValueTypeRef mutable_map =
        ValuePlanFactory::instance().type_for(registry.mutable_map(int_schema, int_schema));
    const DebugDescriptor *map_debug = mutable_map.record()->debug;
    REQUIRE(map_debug->layout == DebugLayoutKind::KeyedSlots);
    REQUIRE(map_debug->key_type == int_type.record());
    REQUIRE(map_debug->element_type == int_type.record());
    REQUIRE(map_debug->dynamic_layout->kind == DebugDynamicKind::StableSlots);
    REQUIRE(has_flag(map_debug->dynamic_layout->flags, DebugDynamicFlags::HasSlotState));
    REQUIRE(has_flag(map_debug->dynamic_layout->flags, DebugDynamicFlags::KeyDataIsPointerTable));
}

TEST_CASE("ValueOps: ops_for<T> returns a stable canonical vtable")
{
    using namespace hgraph;

    static_assert(!std::is_copy_constructible_v<ValueView>);
    static_assert(!std::is_copy_assignable_v<ValueView>);
    static_assert(std::is_move_constructible_v<ValueView>);
    static_assert(!std::is_copy_constructible_v<TupleView>);
    static_assert(!std::is_copy_constructible_v<BundleView>);
    static_assert(!std::is_copy_constructible_v<ListView>);
    static_assert(!std::is_copy_constructible_v<SetView>);
    static_assert(!std::is_copy_constructible_v<MapView>);
    static_assert(!std::is_copy_constructible_v<CyclicBufferView>);
    static_assert(!std::is_copy_constructible_v<QueueView>);
    static_assert(!std::is_copy_constructible_v<MutableTupleView>);
    static_assert(!std::is_copy_constructible_v<MutableBundleView>);
    static_assert(!std::is_copy_constructible_v<MutableListView>);
    static_assert(!std::is_copy_constructible_v<MutableCyclicBufferView>);
    static_assert(!std::is_copy_constructible_v<MutableQueueView>);

    REQUIRE(&ops_for<std::int32_t>() == &ops_for<std::int32_t>());
    REQUIRE(&ops_for<std::int32_t>() != &ops_for<double>());

    const ValueOps &ops = ops_for<std::int32_t>();
    REQUIRE(ops.hash_impl != nullptr);
    REQUIRE(ops.equals_impl != nullptr);
    REQUIRE(ops.compare_impl != nullptr);
    REQUIRE(ops.to_string_impl != nullptr);

    std::int32_t a = 42;
    std::int32_t b = 42;
    std::int32_t c = 7;
    STATIC_REQUIRE(std::is_same_v<decltype(ops.compare(&a, &b)), std::partial_ordering>);
    REQUIRE(ops.equals(&a, &b));
    REQUIRE_FALSE(ops.equals(&a, &c));
    REQUIRE(std::is_eq(ops.compare(&a, &b)));
    REQUIRE(std::is_lt(ops.compare(&c, &a)));
    REQUIRE(std::is_gt(ops.compare(&a, &c)));
    REQUIRE(ops.hash(&a) == ops.hash(&b));
    REQUIRE(ops.to_string(&a) == "42");
}

TEST_CASE("ValueOps: floating compare preserves unordered comparison results")
{
    using namespace hgraph;
    const ValueOps &ops = ops_for<double>();

    double value = 1.0;
    double nan   = std::numeric_limits<double>::quiet_NaN();

    REQUIRE(ops.compare(&nan, &value) == std::partial_ordering::unordered);
    REQUIRE(ops.compare(&value, &nan) == std::partial_ordering::unordered);
}

TEST_CASE("ValueOps: bool to_string and string round-trip use type-specific paths")
{
    using namespace hgraph;
    bool t = true;
    bool f = false;
    REQUIRE(ops_for<bool>().to_string(&t) == "true");
    REQUIRE(ops_for<bool>().to_string(&f) == "false");
    REQUIRE(ops_for<bool>().format_string(&t) == "True");
    REQUIRE(ops_for<bool>().format_string(&f) == "False");

    std::string s{"hello"};
    REQUIRE(ops_for<std::string>().to_string(&s) == "hello");
}

TEST_CASE("ValueOps: unsupported scalar operations do not use object bytes as fallback")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *meta = registry.register_scalar<NoValueOpsScalar>("NoValueOpsScalar");
    REQUIRE_FALSE(meta->is_hashable());
    REQUIRE_FALSE(meta->is_equatable());
    REQUIRE_FALSE(meta->is_comparable());

    const ValueOps &ops = ops_for<NoValueOpsScalar>();
    REQUIRE(ops.hash_impl == nullptr);
    REQUIRE(ops.equals_impl == nullptr);
    REQUIRE(ops.compare_impl == nullptr);

    NoValueOpsScalar a{1};
    NoValueOpsScalar b{1};
    REQUIRE_THROWS_AS(ops.hash(&a), std::logic_error);
    REQUIRE_FALSE(ops.equals(&a, &b));
    REQUIRE(ops.equals(&a, &a));
    REQUIRE(ops.compare(&a, &b) == std::partial_ordering::unordered);
    REQUIRE(std::is_eq(ops.compare(&a, &a)));

    Value value{NoValueOpsScalar{1}};
    REQUIRE_THROWS_AS(value.hash(), std::logic_error);
    REQUIRE_THROWS_AS(ValueView{}.hash(), std::logic_error);
}

TEST_CASE("TypeRegistry::register_scalar pairs the schema with a binding")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *meta     = registry.register_scalar<std::int32_t>("int32");

    const auto binding = registry.scalar_type<std::int32_t>();
    REQUIRE(binding != nullptr);
    REQUIRE(binding.valid());
    REQUIRE(binding.schema() == meta);
    REQUIRE(binding.plan() == &MemoryUtils::plan_for<std::int32_t>());
    REQUIRE(binding.ops() == &ops_for<std::int32_t>());

    // Idempotency: re-registering returns the same canonical type record.
    (void)registry.register_scalar<std::int32_t>("int32");
    REQUIRE(registry.scalar_type<std::int32_t>() == binding);
}

TEST_CASE("TypeRegistry::scalar_type returns null for unregistered types")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    REQUIRE(registry.scalar_type<UnregisteredScalar>() == nullptr);
}

TEST_CASE("intern_value_type rejects advertised semantics without implementation hooks")
{
    using namespace hgraph;

    SECTION("equality")
    {
        ValueTypeMetaData schema{ValueTypeKind::Atomic, ValueTypeFlags::Equatable, "missing equality"};
        ValueOps ops = ops_for<std::int32_t>();
        ops.equals_impl = nullptr;
        REQUIRE_THROWS_AS(intern_value_type(schema, MemoryUtils::plan_for<std::int32_t>(), ops),
                          std::invalid_argument);
    }
    SECTION("comparison")
    {
        ValueTypeMetaData schema{ValueTypeKind::Atomic, ValueTypeFlags::Comparable, "missing comparison"};
        ValueOps ops = ops_for<std::int32_t>();
        ops.compare_impl = nullptr;
        REQUIRE_THROWS_AS(intern_value_type(schema, MemoryUtils::plan_for<std::int32_t>(), ops),
                          std::invalid_argument);
    }
    SECTION("hashing")
    {
        ValueTypeMetaData schema{ValueTypeKind::Atomic, ValueTypeFlags::Hashable, "missing hashing"};
        ValueOps ops = ops_for<std::int32_t>();
        ops.hash_impl = nullptr;
        REQUIRE_THROWS_AS(intern_value_type(schema, MemoryUtils::plan_for<std::int32_t>(), ops),
                          std::invalid_argument);
    }
}

TEST_CASE("value destructibility capability follows the storage plan")
{
    using namespace hgraph;

    SECTION("schema triviality does not override a non-destructible plan")
    {
        ValueTypeMetaData schema{ValueTypeKind::Atomic,
                                 ValueTypeFlags::TriviallyDestructible,
                                 "schema-only trivial destructor"};
        auto plan = MemoryUtils::plan_for<std::int32_t>();
        plan.trivially_destructible = false;
        plan.lifecycle.destroy = nullptr;

        REQUIRE_FALSE(has_capability(value_type_capabilities(schema, plan, ops_for<std::int32_t>()),
                                     TypeCapabilities::Destructible));
    }

    SECTION("plan triviality is sufficient when the schema does not advertise it")
    {
        ValueTypeMetaData schema{ValueTypeKind::Atomic, ValueTypeFlags::None,
                                 "plan-only trivial destructor"};
        auto plan = MemoryUtils::plan_for<std::int32_t>();
        plan.trivially_destructible = true;
        plan.lifecycle.destroy = nullptr;

        REQUIRE(has_capability(value_type_capabilities(schema, plan, ops_for<std::int32_t>()),
                               TypeCapabilities::Destructible));
    }
}

TEST_CASE("ValueTypeRef is canonical by record and narrows generic pointers safely")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const ValueTypeRef scalar_type = registry.scalar_type<std::int32_t>();
    const ValueTypeRef factory_type = ValuePlanFactory::instance().type_for(int_meta);
    REQUIRE(scalar_type);
    REQUIRE(scalar_type == factory_type);
    REQUIRE(scalar_type.record() == factory_type.record());

    std::int32_t payload = 42;
    const ValuePtr read_only = scalar_type.read_only(&payload);
    const ValuePtr writable = scalar_type.writable(&payload);
    const ValuePtr typed_null = scalar_type.typed_null();
    REQUIRE(ValueTypeRef::checked(read_only.to_any()) == scalar_type);
    REQUIRE(ValueTypeRef::checked(writable.to_any()) == scalar_type);
    REQUIRE(ValueTypeRef::checked(typed_null.to_any()) == scalar_type);
    REQUIRE_FALSE(ValueTypeRef::checked(AnyPtr{}));
    REQUIRE(read_only.read_only_access());
    REQUIRE(writable.writable_access());
    REQUIRE(typed_null.is_typed_null());

    SchemaHeader foreign_schema{TypeFamily::Node, 1, "foreign node"};
    std::uint32_t foreign_ops = 1;
    const TypeRecordDefinition foreign_definition{
        .key = TypeRecordKey{.schema = &foreign_schema,
                             .role = TypeRole::Runtime,
                             .plan = &MemoryUtils::plan_for<std::int32_t>(),
                             .ops = &foreign_ops,
                             .debug = nullptr,
                             .implementation_label = {}},
        .ops_abi_version = 1,
        .capabilities = TypeCapabilities::None,
    };
    const TypeRecord &foreign_record = TypeRecordRegistry::instance().intern(foreign_definition);
    REQUIRE_THROWS_AS(ValueTypeRef::checked(AnyPtr::read_only(foreign_record, &payload)),
                      std::invalid_argument);
}

// ``ErasedOwner`` itself has its own coverage in ``test_memory_utils.cpp``;
// here we exercise the value-layer round-trip that uses it through
// ``Value`` and ``ValueTypeRef``.

TEST_CASE("Value: atomic round-trip - construct, view, hash/equals/to_string")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    Value v{42};
    REQUIRE(v.has_value());
    REQUIRE(v.schema() != nullptr);
    REQUIRE(v.schema()->value_kind() == ValueTypeKind::Atomic);

    // Owning-value accessors.
    REQUIRE(v.as<std::int32_t>() == 42);
    REQUIRE(*v.try_as<std::int32_t>() == 42);
    REQUIRE(v.try_as<double>() == nullptr);  // type mismatch via view -> still fine, atomic but wrong T
    REQUIRE(v.to_string() == "42");

    // ValueView round-trip.
    ValueView view = v.view();
    REQUIRE(view.valid());
    REQUIRE(view.is_atomic());
    REQUIRE(view.checked_as<std::int32_t>() == 42);
    REQUIRE(view.hash() == ops_for<std::int32_t>().hash(view.data()));
    REQUIRE(view.to_string() == "42");

    // Mutating access is explicit: a writable view is not mutable until
    // begin_mutation() has opened the scope.
    REQUIRE(view.writable_payload());
    REQUIRE_FALSE(view.mutable_payload());
    REQUIRE(view.can_begin_mutation());
    REQUIRE_THROWS_AS(view.set<std::int32_t>(99), std::logic_error);

    auto mutation = view.begin_mutation();
    REQUIRE(mutation.mutable_payload());
    mutation.as<std::int32_t>() = 99;
    REQUIRE(v.as<std::int32_t>() == 99);

    mutation.set<std::int32_t>(123);
    REQUIRE(v.as<std::int32_t>() == 123);
    REQUIRE(view.is_scalar_type<std::int32_t>());
    REQUIRE_FALSE(view.is_scalar_type<double>());
    REQUIRE(view.is_type(v.schema()));

    const Value &const_ref = v;
    ValueView read_only = const_ref.view();
    REQUIRE(read_only.valid());
    REQUIRE_FALSE(read_only.mutable_payload());
    REQUIRE(std::as_const(read_only).checked_as<std::int32_t>() == 123);
    REQUIRE_THROWS_AS(read_only.set<std::int32_t>(456), std::logic_error);
    REQUIRE_THROWS_AS(read_only.checked_mutable_as<std::int32_t>() = 456, std::logic_error);
    REQUIRE(v.as<std::int32_t>() == 123);
}

TEST_CASE("ValueView kind predicates reject malformed compact kinds without throwing")
{
    using namespace hgraph;

    ValueTypeMetaData malformed{ValueTypeKind::Atomic, ValueTypeFlags::None, "malformed"};
    malformed.header.kind = static_cast<TypeKind>(ValueTypeKind::Any) + 1;
    const ValueTypeRef binding = intern_value_type(
        malformed, MemoryUtils::plan_for<std::int32_t>(), ops_for<std::int32_t>());
    const std::int32_t payload = 42;
    ValueView view{binding, &payload};

    ValueTypeMetaData other_malformed{ValueTypeKind::Atomic, ValueTypeFlags::None, "other malformed"};
    other_malformed.header.kind = static_cast<TypeKind>(ValueTypeKind::Any) + 2;
    const ValueTypeRef other_binding = intern_value_type(
        other_malformed, MemoryUtils::plan_for<std::int32_t>(), ops_for<std::int32_t>());
    const std::int32_t other_payload = 42;
    ValueView other_view{other_binding, &other_payload};

    STATIC_REQUIRE(noexcept(view.is_atomic()));
    STATIC_REQUIRE(noexcept(view.is_indexed()));
    STATIC_REQUIRE(noexcept(view.holds_alternative<std::int32_t>()));
    REQUIRE(view.valid());
    REQUIRE_FALSE(view.is_atomic());
    REQUIRE_FALSE(view.is_tuple());
    REQUIRE_FALSE(view.is_bundle());
    REQUIRE_FALSE(view.is_list());
    REQUIRE_FALSE(view.is_set());
    REQUIRE_FALSE(view.is_map());
    REQUIRE_FALSE(view.is_cyclic_buffer());
    REQUIRE_FALSE(view.is_queue());
    REQUIRE_FALSE(view.is_any());
    REQUIRE_FALSE(view.is_indexed());
    REQUIRE_FALSE(view.holds_alternative<std::int32_t>());
    REQUIRE_FALSE(view.is_scalar_type<std::int32_t>());
    REQUIRE_FALSE(view.equals(other_view));
    REQUIRE(view.compare(other_view) == std::partial_ordering::unordered);

    REQUIRE(operator_type_resolution::collection_element_schema(&malformed) == nullptr);
    REQUIRE(operator_type_resolution::homogeneous_tuple_element_schema(&malformed) == nullptr);
    REQUIRE(operator_type_resolution::tuple_element_schema(&malformed) == nullptr);
    REQUIRE(type_resolution_detail::homogeneous_tuple_element(&malformed) == nullptr);

    TSValueTypeMetaData malformed_ts{TSTypeKind::TS, &malformed};
    malformed_ts.value_schema = &malformed;
    REQUIRE(operator_type_resolution::ts_map_value_schema(&malformed_ts) == nullptr);
    REQUIRE_FALSE(current_value_schema_compatible(malformed_ts, other_malformed));
}

TEST_CASE("Value: equality and ordering through bound ValueOps")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<double>("double");

    Value a{10};
    Value b{10};
    Value c{20};
    Value d{10.0};

    STATIC_REQUIRE(std::is_same_v<decltype(a.compare(b)), std::partial_ordering>);
    REQUIRE(a.equals(b));
    REQUIRE_FALSE(a.equals(c));
    REQUIRE(std::is_eq(a.compare(b)));
    REQUIRE(std::is_lt(a.compare(c)));
    REQUIRE(std::is_gt(c.compare(a)));
    REQUIRE(a.compare(d) == std::partial_ordering::unordered);
    REQUIRE(a.hash() == b.hash());
    REQUIRE(a.hash() != c.hash());

    Value empty;
    REQUIRE(std::is_eq(empty.compare(empty)));
    REQUIRE(std::is_lt(empty.compare(a)));
    REQUIRE(std::is_gt(a.compare(empty)));
    REQUIRE(std::is_eq(ValueView{}.compare(ValueView{})));
    REQUIRE(std::is_lt(ValueView{}.compare(a.view())));
    REQUIRE(std::is_gt(a.view().compare(ValueView{})));
    REQUIRE(a.view().compare(d.view()) == std::partial_ordering::unordered);

    Value null_int{*int_meta};
    Value null_int_2{*int_meta};
    const auto *double_meta = registry.value_type("double");
    Value null_double{*double_meta};
    REQUIRE(null_int.equals(null_int_2));
    REQUIRE_FALSE(null_int.equals(null_double));
    REQUIRE(std::is_eq(null_int.compare(null_int_2)));
    REQUIRE(null_int.compare(null_double) == std::partial_ordering::unordered);
    REQUIRE(std::is_lt(null_int.compare(a)));
    REQUIRE(std::is_gt(a.compare(null_int)));
}

TEST_CASE("ValueView: clone and copy_from preserve binding and payload")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<double>("double");

    Value source{42};
    Value cloned = source.view().clone();
    REQUIRE(cloned.binding() == source.binding());
    REQUIRE(cloned.as<std::int32_t>() == 42);
    cloned.begin_mutation().as<std::int32_t>() = 7;
    REQUIRE(source.as<std::int32_t>() == 42);

    Value target{0};
    target.begin_mutation().copy_from(source.view());
    REQUIRE(target.as<std::int32_t>() == 42);
    target.assign_from(cloned.view());
    REQUIRE(target.as<std::int32_t>() == 7);

    Value other_type{3.0};
    REQUIRE_FALSE(target.begin_mutation().try_copy_from(other_type.view()));
    REQUIRE_THROWS_AS(target.begin_mutation().copy_from(other_type.view()), std::invalid_argument);

    Value typed_null{*int_meta};
    Value typed_null_clone = typed_null.view().clone();
    REQUIRE_FALSE(typed_null_clone.has_value());
    REQUIRE(typed_null_clone.binding() == typed_null.binding());
    REQUIRE_THROWS_AS(ValueView{}.clone(), std::logic_error);
}

TEST_CASE("Value: default-constructed Value has no payload")
{
    using namespace hgraph;
    Value v;
    REQUIRE_FALSE(v.has_value());
    REQUIRE(v.schema() == nullptr);
    REQUIRE(v.view().valid() == false);
}

TEST_CASE("Value: Value(schema) preserves binding in typed-null state")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    Value v{*int_meta};
    REQUIRE_FALSE(v.has_value());
    REQUIRE(v.schema() == int_meta);
    REQUIRE(v.binding() == ValuePlanFactory::instance().type_for(int_meta));
    REQUIRE_FALSE(v.view().valid());

    Value copy = v;
    REQUIRE_FALSE(copy.has_value());
    REQUIRE(copy.schema() == int_meta);
    REQUIRE(copy.binding() == v.binding());
}

TEST_CASE("Value: Value(binding) builds a default-valued payload of the bound type")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<double>("double");

    ValueTypeRef binding = registry.scalar_type<double>();
    REQUIRE(binding != nullptr);

    Value v{binding};
    REQUIRE(v.has_value());
    REQUIRE(v.schema() != nullptr);
    REQUIRE(v.schema()->value_kind() == ValueTypeKind::Atomic);
    REQUIRE(v.as<double>() == 0.0);  // default-constructed double

    v.reset();
    REQUIRE_FALSE(v.has_value());
    REQUIRE(v.schema() != nullptr);
    REQUIRE(v.schema()->value_kind() == ValueTypeKind::Atomic);
}

TEST_CASE("Value: move construction transfers ownership")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    Value original{123};
    Value moved{std::move(original)};
    REQUIRE_FALSE(original.has_value());
    REQUIRE(moved.has_value());
    REQUIRE(moved.as<std::int32_t>() == 123);
}

TEST_CASE("Value: throws when a scalar type has not been registered")
{
    using namespace hgraph;
    REQUIRE_THROWS_AS(Value(UnregisteredScalar{}), std::logic_error);
}
