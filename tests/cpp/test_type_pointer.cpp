#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/type_pointer.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>
#include <utility>

namespace
{
    struct MockOps
    {
        std::uint32_t marker{0};
    };

    struct RawAnyPtr
    {
        std::uintptr_t type_bits{0};
        const void *data{nullptr};
    };

    [[nodiscard]] hgraph::TypeRole role_for(hgraph::TypeFamily family)
    {
        switch (family)
        {
        case hgraph::TypeFamily::Value:
            return hgraph::TypeRole::Instance;
        case hgraph::TypeFamily::TimeSeries:
            return hgraph::TypeRole::Data;
        case hgraph::TypeFamily::Node:
        case hgraph::TypeFamily::Graph:
        case hgraph::TypeFamily::Executor:
        case hgraph::TypeFamily::Clock:
            return hgraph::TypeRole::Runtime;
        case hgraph::TypeFamily::Invalid:
            return hgraph::TypeRole::Invalid;
        }
        return hgraph::TypeRole::Invalid;
    }

    [[nodiscard]] const hgraph::TypeRecord &
    intern_record(const hgraph::SchemaHeader &schema, MockOps &ops,
                  hgraph::TypeCapabilities capabilities = hgraph::TypeCapabilities::None,
                  std::string_view implementation_label = "mock implementation",
                  const hgraph::MemoryUtils::StoragePlan *plan = &hgraph::MemoryUtils::plan_for<std::uint32_t>(),
                  const hgraph::DebugDescriptor *debug = nullptr)
    {
        return hgraph::TypeRecordRegistry::instance().intern(
            {{&schema, role_for(schema.family), plan, &ops, debug}, 1, capabilities, implementation_label});
    }

    [[nodiscard]] hgraph::AnyPtr raw_pointer(const hgraph::TypeRecord *record, const void *data, std::uintptr_t tag)
    {
        const auto bits = reinterpret_cast<std::uintptr_t>(record) | tag;
        return std::bit_cast<hgraph::AnyPtr>(RawAnyPtr{bits, data});
    }

    template <typename T> constexpr void assert_typed_layout()
    {
        static_assert(sizeof(T) == 2 * sizeof(void *));
        static_assert(alignof(T) == alignof(void *));
        static_assert(std::is_standard_layout_v<T>);
        static_assert(std::is_trivially_copyable_v<T>);
    }

    template <typename T>
    concept HasAsWritable = requires(const T &value) { value.as_writable(); };

    template <typename T>
    concept HasRawRecordConstructor = requires(const hgraph::TypeRecord *record, const void *data) { T{record, data}; };

    struct CountedPayload
    {
        int *destructions;
        ~CountedPayload() { ++*destructions; }
    };
} // namespace

TEST_CASE("type pointer access tags and layouts are fixed", "[type-erasure][type-pointer]")
{
    using namespace hgraph;

    STATIC_REQUIRE(sizeof(AccessMode) == sizeof(std::uintptr_t));
    STATIC_REQUIRE(static_cast<std::uintptr_t>(AccessMode::ReadOnly) == 0);
    STATIC_REQUIRE(static_cast<std::uintptr_t>(AccessMode::Writable) == 1);
    STATIC_REQUIRE(static_cast<std::uintptr_t>(AccessMode::Mutation) == 2);
    STATIC_REQUIRE(TaggedTypeRecordPtr::tag_mask == 3);
    STATIC_REQUIRE(alignof(TypeRecord) >= 4);

    STATIC_REQUIRE(sizeof(RawAnyPtr) == sizeof(AnyPtr));
    STATIC_REQUIRE(detail::TypePointerLayoutAccess::type_offset == 0);
    STATIC_REQUIRE(detail::TypePointerLayoutAccess::data_offset == sizeof(void *));
    STATIC_REQUIRE(sizeof(AnyPtr) == 2 * sizeof(void *));
    STATIC_REQUIRE(alignof(AnyPtr) == alignof(void *));
    STATIC_REQUIRE(std::is_standard_layout_v<AnyPtr>);
    STATIC_REQUIRE(std::is_trivially_copyable_v<AnyPtr>);

    assert_typed_layout<ValuePtr>();
    assert_typed_layout<TSDataPtr>();
    assert_typed_layout<NodePtr>();
    assert_typed_layout<GraphPtr>();
    assert_typed_layout<ExecutorPtr>();
    assert_typed_layout<ClockPtr>();
    assert_typed_layout<TypedPtr<TypeFamily::Value>>();
}

TEST_CASE("default type pointers are constexpr unbound values", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    constexpr AnyPtr any{};
    constexpr ValuePtr typed{};

    STATIC_REQUIRE(any.is_unbound());
    STATIC_REQUIRE(typed.is_unbound());
    REQUIRE(any.well_formed());
    REQUIRE_FALSE(any.bound());
    REQUIRE_FALSE(any.is_typed_null());
    REQUIRE_FALSE(any.has_value());
    REQUIRE_FALSE(any.valid());
    REQUIRE_FALSE(static_cast<bool>(any));
    REQUIRE(any.record() == nullptr);
    REQUIRE(any.data() == nullptr);
    REQUIRE(any.access_mode() == AccessMode::ReadOnly);
}

TEST_CASE("typed null is readonly and supported for every settled family role", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    constexpr std::array families{TypeFamily::Value, TypeFamily::TimeSeries, TypeFamily::Node,
                                  TypeFamily::Graph, TypeFamily::Executor,   TypeFamily::Clock};
    std::array<MockOps, families.size()> operations{};

    for (std::size_t index = 0; index < families.size(); ++index)
    {
        SchemaHeader schema{families[index], TYPE_KIND_NONE, "mock"};
        const TypeRecord &record = intern_record(schema, operations[index]);
        const AnyPtr pointer = AnyPtr::typed_null(record);
        REQUIRE(pointer.well_formed());
        REQUIRE(pointer.bound());
        REQUIRE(pointer.is_typed_null());
        REQUIRE_FALSE(pointer.has_value());
        REQUIRE_FALSE(pointer.valid());
        REQUIRE(pointer.read_only_access());
        REQUIRE_FALSE(pointer.writable_access());
        REQUIRE(pointer.record() == &record);
        REQUIRE(pointer.data() == nullptr);
    }
}

TEST_CASE("live readonly and writable factories preserve borrowed data", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 9, "value"};
    MockOps ops{};
    const TypeRecord &record = intern_record(schema, ops, TypeCapabilities::Mutable);
    int payload = 42;

    const AnyPtr readonly = AnyPtr::read_only(record, &payload);
    REQUIRE(readonly.well_formed());
    REQUIRE(readonly.valid());
    REQUIRE(readonly.has_value());
    REQUIRE(readonly.read_only_access());
    REQUIRE_FALSE(readonly.writable_access());
    REQUIRE(readonly.data() == &payload);

    const AnyPtr writable = AnyPtr::writable(record, &payload);
    REQUIRE(writable.well_formed());
    REQUIRE(writable.valid());
    REQUIRE(writable.writable_access());
    REQUIRE_FALSE(writable.read_only_access());
    REQUIRE(writable.data() == &payload);
}

TEST_CASE("live factories reject null data", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "value"};
    MockOps ops{};
    const TypeRecord &record = intern_record(schema, ops);

    REQUIRE_THROWS_AS(AnyPtr::read_only(record, nullptr), std::invalid_argument);
    REQUIRE_THROWS_AS(AnyPtr::writable(record, nullptr), std::invalid_argument);
}

TEST_CASE("factories reject corrupt records and schema headers", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "value"};
    MockOps ops{};
    const TypeRecord &canonical = intern_record(schema, ops);
    TypeRecord record = canonical;
    int payload{};

    SECTION("record magic") { record.magic = 0; }
    SECTION("record ABI") { record.abi_version = 0; }
    SECTION("schema magic") { schema.magic = 0; }
    SECTION("schema ABI") { schema.abi_version = 0; }

    REQUIRE_THROWS_AS(AnyPtr::read_only(record, &payload), std::invalid_argument);
    REQUIRE_THROWS_AS(AnyPtr::typed_null(record), std::invalid_argument);
}

TEST_CASE("malformed raw pointer states are observable but never accepted", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "value"};
    MockOps ops{};
    const TypeRecord &record = intern_record(schema, ops);
    int payload{};

    const AnyPtr null_record = raw_pointer(nullptr, &payload, 0);
    REQUIRE_FALSE(null_record.well_formed());
    REQUIRE_FALSE(null_record.bound());
    REQUIRE_FALSE(null_record.valid());
    REQUIRE_THROWS_AS(ValuePtr::checked(null_record), std::invalid_argument);

    const AnyPtr invalid_tag = raw_pointer(&record, &payload, 3);
    REQUIRE(invalid_tag.access_mode() == static_cast<AccessMode>(3));
    REQUIRE_FALSE(invalid_tag.well_formed());
    REQUIRE_FALSE(invalid_tag.bound());
    REQUIRE_FALSE(invalid_tag.has_value());
    REQUIRE_FALSE(invalid_tag.valid());
    REQUIRE(invalid_tag.schema() == nullptr);
    REQUIRE(invalid_tag.plan() == nullptr);
    REQUIRE(invalid_tag.ops() == nullptr);
    REQUIRE(invalid_tag.family() == TypeFamily::Invalid);
    REQUIRE(invalid_tag.role() == TypeRole::Invalid);
    REQUIRE(invalid_tag.capabilities() == TypeCapabilities::None);
    REQUIRE(invalid_tag.effective_name().empty());
    REQUIRE_THROWS_AS(ValuePtr::checked(invalid_tag), std::invalid_argument);

    for (std::uintptr_t tag : {std::uintptr_t{1}, std::uintptr_t{2}})
    {
        const AnyPtr invalid_null = raw_pointer(&record, nullptr, tag);
        REQUIRE_FALSE(invalid_null.well_formed());
        REQUIRE_FALSE(invalid_null.bound());
        REQUIRE_FALSE(invalid_null.is_typed_null());
        REQUIRE_FALSE(invalid_null.has_value());
        REQUIRE_FALSE(invalid_null.valid());
        REQUIRE_THROWS_AS(ValuePtr::checked(invalid_null), std::invalid_argument);
    }
}

TEST_CASE("raw mutation state requires a mutable type record", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "fixed"};
    MockOps ops{};
    const TypeRecord &record = intern_record(schema, ops, TypeCapabilities::None);
    int payload{};
    const AnyPtr mutation = raw_pointer(&record, &payload, static_cast<std::uintptr_t>(AccessMode::Mutation));

    REQUIRE_FALSE(mutation.well_formed());
    REQUIRE_FALSE(mutation.bound());
    REQUIRE_FALSE(mutation.has_value());
    REQUIRE_FALSE(mutation.valid());
    REQUIRE_FALSE(static_cast<bool>(mutation));
    REQUIRE_THROWS_AS(mutation.mutable_data(), std::logic_error);
    REQUIRE_THROWS_AS(ValuePtr::checked(mutation), std::invalid_argument);
}

TEST_CASE("post-construction record corruption invalidates borrowed pointers", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "value"};
    MockOps ops{};
    TypeRecord record = intern_record(schema, ops);
    int payload{};
    const AnyPtr pointer = AnyPtr::read_only(record, &payload);
    REQUIRE(pointer.valid());

    SECTION("record") { record.magic = 0; }
    SECTION("schema") { schema.magic = 0; }

    REQUIRE_FALSE(pointer.well_formed());
    REQUIRE_FALSE(pointer.bound());
    REQUIRE_FALSE(pointer.valid());
    REQUIRE(pointer.schema() == nullptr);
    REQUIRE(pointer.plan() == nullptr);
    REQUIRE(pointer.ops() == nullptr);
    REQUIRE(pointer.family() == TypeFamily::Invalid);
    REQUIRE(pointer.role() == TypeRole::Invalid);
    REQUIRE(pointer.capabilities() == TypeCapabilities::None);
    REQUIRE(pointer.semantic_name().empty());
}

TEST_CASE("shallow inspection forwards common record metadata for live and typed-null pointers",
          "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Node, 17, "semantic node"};
    MockOps ops{7};
    int debug_token{};
    const auto *debug = reinterpret_cast<const DebugDescriptor *>(&debug_token);
    const TypeRecord &record = intern_record(schema, ops, TypeCapabilities::Mutable | TypeCapabilities::Viewable,
                                             "native node", &MemoryUtils::plan_for<std::uint64_t>(), debug);
    std::uint64_t payload{};

    for (AnyPtr pointer : {AnyPtr::read_only(record, &payload), AnyPtr::typed_null(record)})
    {
        REQUIRE(pointer.schema() == &schema);
        REQUIRE(pointer.plan() == &MemoryUtils::plan_for<std::uint64_t>());
        REQUIRE(pointer.ops() == &ops);
        REQUIRE(pointer.debug() == debug);
        REQUIRE(pointer.family() == TypeFamily::Node);
        REQUIRE(pointer.role() == TypeRole::Runtime);
        REQUIRE(pointer.kind() == 17);
        REQUIRE(pointer.capabilities() == (TypeCapabilities::Mutable | TypeCapabilities::Viewable));
        REQUIRE(static_cast<bool>(pointer.semantic_name() == "semantic node"));
        REQUIRE(static_cast<bool>(pointer.implementation_name() == "native node"));
        REQUIRE(static_cast<bool>(pointer.effective_name() == "native node"));
    }

    const AnyPtr unbound{};
    REQUIRE(unbound.schema() == nullptr);
    REQUIRE(unbound.plan() == nullptr);
    REQUIRE(unbound.ops() == nullptr);
    REQUIRE(unbound.debug() == nullptr);
    REQUIRE(unbound.classification().family == TypeFamily::Invalid);
    REQUIRE(unbound.capabilities() == TypeCapabilities::None);
    REQUIRE(unbound.effective_name().empty());
}

TEST_CASE("readonly downgrade is one-way and preserves identity", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "value"};
    MockOps ops{};
    const TypeRecord &record = intern_record(schema, ops, TypeCapabilities::Mutable);
    int payload{};

    const AnyPtr writable = AnyPtr::writable(record, &payload);
    const AnyPtr readonly = writable.as_read_only();
    REQUIRE(readonly == writable);
    REQUIRE(readonly.read_only_access());
    REQUIRE_FALSE(readonly.same_access_as(writable));

    const AnyPtr mutation = writable.begin_mutation();
    const AnyPtr mutation_readonly = mutation.as_read_only();
    REQUIRE(mutation_readonly == mutation);
    REQUIRE(mutation_readonly.read_only_access());
    REQUIRE(AnyPtr{}.as_read_only().is_unbound());
    REQUIRE(AnyPtr::typed_null(record).as_read_only().is_typed_null());

    const AnyPtr malformed = raw_pointer(&record, &payload, 3);
    REQUIRE_THROWS_AS(malformed.as_read_only(), std::logic_error);
}

TEST_CASE("public pointer APIs cannot escalate or construct raw states", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    STATIC_REQUIRE_FALSE(HasAsWritable<AnyPtr>);
    STATIC_REQUIRE_FALSE(HasAsWritable<ValuePtr>);
    STATIC_REQUIRE_FALSE(HasRawRecordConstructor<AnyPtr>);
    STATIC_REQUIRE_FALSE(std::is_constructible_v<AnyPtr, const TypeRecord *, const void *, AccessMode>);
}

TEST_CASE("mutation begins only from live writable mutable data", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader mutable_schema{TypeFamily::Value, 1, "mutable"};
    SchemaHeader fixed_schema{TypeFamily::Value, 2, "fixed"};
    MockOps mutable_ops{};
    MockOps fixed_ops{};
    TypeRecord mutable_record = intern_record(mutable_schema, mutable_ops, TypeCapabilities::Mutable);
    const TypeRecord &fixed_record = intern_record(fixed_schema, fixed_ops);
    int payload{};

    const AnyPtr mutation = AnyPtr::writable(mutable_record, &payload).begin_mutation();
    REQUIRE(mutation.valid());
    REQUIRE(mutation.mutation_access());
    REQUIRE(mutation.writable_access());

    REQUIRE_THROWS_AS(AnyPtr::read_only(mutable_record, &payload).begin_mutation(), std::logic_error);
    REQUIRE_THROWS_AS(AnyPtr::typed_null(mutable_record).begin_mutation(), std::logic_error);
    REQUIRE_THROWS_AS(AnyPtr{}.begin_mutation(), std::logic_error);
    REQUIRE_THROWS_AS(AnyPtr::writable(fixed_record, &payload).begin_mutation(), std::logic_error);

    const AnyPtr corrupt = AnyPtr::writable(mutable_record, &payload);
    mutable_record.magic = 0;
    REQUIRE_THROWS_AS(corrupt.begin_mutation(), std::logic_error);
}

TEST_CASE("mutation transitions are idempotent and expose mutable data only in mutation",
          "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "value"};
    MockOps ops{};
    const TypeRecord &record = intern_record(schema, ops, TypeCapabilities::Mutable);
    int payload{};

    const AnyPtr writable = AnyPtr::writable(record, &payload);
    const AnyPtr mutation = writable.begin_mutation();
    REQUIRE(mutation.begin_mutation().same_state_as(mutation));
    REQUIRE(mutation.mutable_data() == &payload);

    const AnyPtr ended = mutation.end_mutation();
    REQUIRE(ended == mutation);
    REQUIRE(ended.access_mode() == AccessMode::Writable);
    REQUIRE_THROWS_AS(ended.end_mutation(), std::logic_error);
    REQUIRE_THROWS_AS(writable.mutable_data(), std::logic_error);
    REQUIRE_THROWS_AS(AnyPtr::typed_null(record).mutable_data(), std::logic_error);
}

TEST_CASE("pointer equality ignores access but retains record and data identity", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader first_schema{TypeFamily::Value, 1, "first"};
    SchemaHeader second_schema{TypeFamily::Value, 2, "second"};
    MockOps first_ops{};
    MockOps second_ops{};
    const TypeRecord &first_record = intern_record(first_schema, first_ops, TypeCapabilities::Mutable);
    const TypeRecord &second_record = intern_record(second_schema, second_ops, TypeCapabilities::Mutable);
    int first_payload{};
    int second_payload{};

    const AnyPtr readonly = AnyPtr::read_only(first_record, &first_payload);
    const AnyPtr writable = AnyPtr::writable(first_record, &first_payload);
    const AnyPtr mutation = writable.begin_mutation();
    REQUIRE(readonly == writable);
    REQUIRE(readonly == mutation);
    REQUIRE_FALSE(readonly.same_access_as(writable));
    REQUIRE_FALSE(readonly.same_state_as(writable));
    REQUIRE(writable.same_access_as(AnyPtr::writable(first_record, &second_payload)));
    REQUIRE_FALSE(readonly == AnyPtr::read_only(first_record, &second_payload));
    REQUIRE_FALSE(readonly == AnyPtr::read_only(second_record, &first_payload));
    REQUIRE(AnyPtr{} == AnyPtr{});
    REQUIRE(AnyPtr::typed_null(first_record) == AnyPtr::typed_null(first_record));
    REQUIRE_FALSE(AnyPtr::typed_null(first_record) == AnyPtr::typed_null(second_record));
}

TEST_CASE("typed narrowing accepts every settled family and rejects family and role mismatches",
          "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    int payload{};

    SchemaHeader value_schema{TypeFamily::Value, 1, "value"};
    SchemaHeader ts_schema{TypeFamily::TimeSeries, 2, "ts"};
    SchemaHeader node_schema{TypeFamily::Node, 3, "node"};
    SchemaHeader graph_schema{TypeFamily::Graph, 4, "graph"};
    SchemaHeader executor_schema{TypeFamily::Executor, 5, "executor"};
    SchemaHeader clock_schema{TypeFamily::Clock, 6, "clock"};
    std::array<MockOps, 6> operations{};

    const AnyPtr value = AnyPtr::read_only(intern_record(value_schema, operations[0]), &payload);
    const AnyPtr ts = AnyPtr::read_only(intern_record(ts_schema, operations[1]), &payload);
    const AnyPtr node = AnyPtr::read_only(intern_record(node_schema, operations[2]), &payload);
    const AnyPtr graph = AnyPtr::read_only(intern_record(graph_schema, operations[3]), &payload);
    const AnyPtr executor = AnyPtr::read_only(intern_record(executor_schema, operations[4]), &payload);
    const AnyPtr clock = AnyPtr::read_only(intern_record(clock_schema, operations[5]), &payload);

    REQUIRE(ValuePtr::checked(value).to_any().same_state_as(value));
    REQUIRE(TSDataPtr::checked(ts).to_any().same_state_as(ts));
    REQUIRE(NodePtr::checked(node).to_any().same_state_as(node));
    REQUIRE(GraphPtr::checked(graph).to_any().same_state_as(graph));
    REQUIRE(ExecutorPtr::checked(executor).to_any().same_state_as(executor));
    REQUIRE(ClockPtr::checked(clock).to_any().same_state_as(clock));
    REQUIRE_THROWS_AS(NodePtr::checked(value), std::invalid_argument);
    REQUIRE_THROWS_AS((TypedPtr<TypeFamily::Value, TypeRole::Runtime>::checked(value)), std::invalid_argument);
}

TEST_CASE("family wildcard conversion is unchecked only in the widening direction", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    using ValueFamilyPtr = TypedPtr<TypeFamily::Value>;

    STATIC_REQUIRE(std::is_convertible_v<ValuePtr, AnyPtr>);
    STATIC_REQUIRE(std::is_convertible_v<ValuePtr, ValueFamilyPtr>);
    STATIC_REQUIRE_FALSE(std::is_convertible_v<ValueFamilyPtr, ValuePtr>);
    STATIC_REQUIRE_FALSE(std::is_convertible_v<ValuePtr, NodePtr>);

    SchemaHeader schema{TypeFamily::Value, 1, "value"};
    MockOps ops{};
    const TypeRecord &record = intern_record(schema, ops, TypeCapabilities::Mutable);
    int payload{};
    const AnyPtr any = AnyPtr::writable(record, &payload).begin_mutation();
    const ValuePtr specific = ValuePtr::checked(any);
    const ValueFamilyPtr wildcard = specific;
    const AnyPtr widened = specific;

    REQUIRE(wildcard.to_any().same_state_as(any));
    REQUIRE(widened.same_state_as(any));
    REQUIRE(ValuePtr::checked(wildcard.to_any()).to_any().same_state_as(any));
    REQUIRE(ValueFamilyPtr::checked(any).to_any().same_state_as(any));
}

TEST_CASE("typed narrowing handles unbound and typed-null states", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    REQUIRE(ValuePtr::checked(AnyPtr{}).is_unbound());

    SchemaHeader schema{TypeFamily::Value, 1, "value"};
    MockOps ops{};
    const TypeRecord &record = intern_record(schema, ops);
    const AnyPtr null = AnyPtr::typed_null(record);
    const ValuePtr typed_null = ValuePtr::checked(null);
    REQUIRE(typed_null.is_typed_null());
    REQUIRE(typed_null.to_any().same_state_as(null));
}

TEST_CASE("typed access transitions preserve their specialization", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    STATIC_REQUIRE(std::same_as<decltype(std::declval<ValuePtr>().as_read_only()), ValuePtr>);
    STATIC_REQUIRE(std::same_as<decltype(std::declval<ValuePtr>().begin_mutation()), ValuePtr>);
    STATIC_REQUIRE(std::same_as<decltype(std::declval<ValuePtr>().end_mutation()), ValuePtr>);

    SchemaHeader schema{TypeFamily::Value, 1, "value"};
    MockOps ops{};
    const TypeRecord &record = intern_record(schema, ops, TypeCapabilities::Mutable);
    int payload{};
    const ValuePtr writable = ValuePtr::checked(AnyPtr::writable(record, &payload));
    const ValuePtr mutation = writable.begin_mutation();
    REQUIRE(mutation.mutation_access());
    REQUIRE(mutation.end_mutation().access_mode() == AccessMode::Writable);
    REQUIRE(mutation.as_read_only().read_only_access());
}

TEST_CASE("borrowed pointer copy and move never affect external payload lifetime", "[type-erasure][type-pointer]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "counted"};
    MockOps ops{};
    const TypeRecord &record =
        intern_record(schema, ops, TypeCapabilities::Mutable, "counted", &MemoryUtils::plan_for<CountedPayload>());
    int destructions = 0;
    {
        CountedPayload payload{&destructions};
        const AnyPtr original = AnyPtr::writable(record, &payload);
        AnyPtr copy = original;
        AnyPtr moved = std::move(copy);
        ValuePtr typed = ValuePtr::checked(moved);
        ValuePtr typed_copy = typed;
        ValuePtr typed_moved = std::move(typed_copy);
        REQUIRE(original.data() == &payload);
        REQUIRE(moved.data() == &payload);
        REQUIRE(typed_moved.data() == &payload);
        REQUIRE(destructions == 0);
    }
    REQUIRE(destructions == 1);
}
