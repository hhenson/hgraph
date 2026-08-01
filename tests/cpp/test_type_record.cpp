#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/value/json_codec.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_tostring.hpp>

#include <array>
#include <atomic>
#include <barrier>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <type_traits>

namespace
{
    struct MockSchema
    {
        hgraph::SchemaHeader header;
        std::uint32_t payload{0};
    };

    struct MockOps
    {
        std::uint32_t marker{0};
    };

    [[nodiscard]] const hgraph::DebugDescriptor *debug_address(const void *address)
    {
        return static_cast<const hgraph::DebugDescriptor *>(address);
    }

    [[nodiscard]] hgraph::TypeRecordDefinition
    definition_for(const hgraph::SchemaHeader &schema, hgraph::TypeRole role, const void *ops,
                   std::string_view implementation_label = {},
                   hgraph::TypeCapabilities capabilities = hgraph::TypeCapabilities::None,
                   const hgraph::MemoryUtils::StoragePlan *plan = &hgraph::MemoryUtils::plan_for<std::uint32_t>(),
                   const hgraph::DebugDescriptor *debug = nullptr)
    {
        return {{&schema, role, plan, ops, debug}, 1, capabilities, implementation_label};
    }

    [[nodiscard]] constexpr hgraph::TypeRole allowed_role(hgraph::TypeFamily family)
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
} // namespace

TEST_CASE("type record common enums and capability operations are fixed", "[type-erasure][type-record]")
{
    using namespace hgraph;

    STATIC_REQUIRE(sizeof(TypeFamily) == sizeof(std::uint8_t));
    STATIC_REQUIRE(sizeof(TypeRole) == sizeof(std::uint8_t));
    STATIC_REQUIRE(sizeof(TypeKind) == sizeof(std::uint8_t));
    STATIC_REQUIRE(sizeof(TypeCapabilities) == sizeof(std::uint32_t));

    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeFamily::Invalid) == 0);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeFamily::Value) == 1);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeFamily::TimeSeries) == 2);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeFamily::Node) == 3);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeFamily::Graph) == 4);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeFamily::Executor) == 5);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeFamily::Clock) == 6);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeRole::Invalid) == 0);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeRole::Instance) == 1);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeRole::Data) == 2);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeRole::Runtime) == 3);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeRole::Input) == 4);
    STATIC_REQUIRE(static_cast<std::uint8_t>(TypeRole::Output) == 5);
    STATIC_REQUIRE(TYPE_KIND_NONE == 0xff);
    STATIC_REQUIRE(SCHEMA_HEADER_MAGIC == 0x48475348u);
    STATIC_REQUIRE(TYPE_RECORD_MAGIC == 0x48475452u);
    STATIC_REQUIRE(SCHEMA_HEADER_ABI_VERSION == 1);
    STATIC_REQUIRE(TYPE_RECORD_ABI_VERSION == 1);
    STATIC_REQUIRE(INVALID_OPS_ABI_VERSION == 0);

    STATIC_REQUIRE(static_cast<std::uint32_t>(TypeCapabilities::None) == 0);
    STATIC_REQUIRE(static_cast<std::uint32_t>(TypeCapabilities::Constructible) == (1u << 0u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(TypeCapabilities::Destructible) == (1u << 1u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(TypeCapabilities::Copyable) == (1u << 2u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(TypeCapabilities::Movable) == (1u << 3u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(TypeCapabilities::Mutable) == (1u << 4u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(TypeCapabilities::Equatable) == (1u << 5u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(TypeCapabilities::Comparable) == (1u << 6u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(TypeCapabilities::Hashable) == (1u << 7u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(TypeCapabilities::HasChildren) == (1u << 8u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(TypeCapabilities::Viewable) == (1u << 9u));

    TypeCapabilities capabilities = TypeCapabilities::Constructible | TypeCapabilities::Viewable;
    REQUIRE(has_capability(capabilities, TypeCapabilities::Constructible));
    REQUIRE(has_capability(capabilities, TypeCapabilities::Viewable));
    REQUIRE_FALSE(has_capability(capabilities, TypeCapabilities::Mutable));
    REQUIRE((capabilities & TypeCapabilities::Viewable) == TypeCapabilities::Viewable);
    capabilities |= TypeCapabilities::Mutable;
    REQUIRE(has_capability(capabilities, TypeCapabilities::Mutable));
}

TEST_CASE("debug descriptor common enums and layouts are fixed", "[type-erasure][debug-descriptor]")
{
    using namespace hgraph;

    STATIC_REQUIRE(DEBUG_DESCRIPTOR_MAGIC == 0x48474444u);
    STATIC_REQUIRE(DEBUG_DESCRIPTOR_ABI_VERSION == 3);
    STATIC_REQUIRE(DEBUG_DYNAMIC_LAYOUT_MAGIC == 0x4847444cu);
    STATIC_REQUIRE(DEBUG_DYNAMIC_LAYOUT_ABI_VERSION == 2);
    STATIC_REQUIRE(sizeof(DebugLayoutKind) == sizeof(std::uint8_t));
    STATIC_REQUIRE(sizeof(DebugAtomicKind) == sizeof(std::uint8_t));
    STATIC_REQUIRE(sizeof(DebugDescriptorFlags) == sizeof(std::uint32_t));
    STATIC_REQUIRE(sizeof(DebugFieldFlags) == sizeof(std::uint32_t));
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugLayoutKind::Opaque) == 0);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugLayoutKind::Atomic) == 1);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugLayoutKind::FixedComposite) == 2);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugLayoutKind::Sequence) == 3);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugLayoutKind::KeyedSlots) == 4);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugLayoutKind::Node) == 5);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugLayoutKind::Graph) == 6);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugLayoutKind::TimeSeries) == 7);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugAtomicKind::String) == 5);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugAtomicKind::Opaque) == 0);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugAtomicKind::Boolean) == 1);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugAtomicKind::SignedInteger) == 2);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugAtomicKind::UnsignedInteger) == 3);
    STATIC_REQUIRE(static_cast<std::uint8_t>(DebugAtomicKind::FloatingPoint) == 4);
    STATIC_REQUIRE(static_cast<std::uint32_t>(DebugFieldFlags::EmbeddedPointer) == (1u << 2u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(DebugDynamicFlags::ElementsArePointers) == (1u << 9u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(DebugDynamicFlags::DataPointersAreTagged) == (1u << 10u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(DebugDynamicFlags::KeyPointersAreTagged) == (1u << 11u));
    STATIC_REQUIRE(static_cast<std::uint32_t>(DebugDynamicFlags::SlotStateIsTaggedPointer) == (1u << 12u));

    DebugDescriptor descriptor{
        .magic = DEBUG_DESCRIPTOR_MAGIC,
        .abi_version = DEBUG_DESCRIPTOR_ABI_VERSION,
        .layout = DebugLayoutKind::Atomic,
        .atomic_kind = DebugAtomicKind::SignedInteger,
    };
    REQUIRE(descriptor.valid());
    descriptor.reserved0 = 1;
    REQUIRE_FALSE(descriptor.valid());

    DebugDynamicLayout dynamic{
        .magic = DEBUG_DYNAMIC_LAYOUT_MAGIC,
        .abi_version = DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
        .kind = DebugDynamicKind::Contiguous,
        .flags = DebugDynamicFlags::SizeIsConstant,
        .size_constant = 3,
        .stride = sizeof(std::int32_t),
    };
    REQUIRE(dynamic.valid());
    dynamic.flags = DebugDynamicFlags::DataIsPointerTable;
    REQUIRE_FALSE(dynamic.valid());

    DebugField pointer_field{
        .name = "graph",
        .flags = DebugFieldFlags::EmbeddedPointer,
    };
    descriptor = DebugDescriptor{
        .magic = DEBUG_DESCRIPTOR_MAGIC,
        .abi_version = DEBUG_DESCRIPTOR_ABI_VERSION,
        .layout = DebugLayoutKind::Node,
        .field_count = 1,
        .fields = &pointer_field,
    };
    REQUIRE(descriptor.valid());
    pointer_field.flags = DebugFieldFlags::EmbeddedOwner | DebugFieldFlags::EmbeddedPointer;
    REQUIRE_FALSE(descriptor.valid());

    dynamic.kind = DebugDynamicKind::StableSlots;
    dynamic.flags = DebugDynamicFlags::DataIsIndirect | DebugDynamicFlags::DataIsPointerTable |
                    DebugDynamicFlags::ElementsAreOwners;
    dynamic.size_offset = sizeof(std::size_t);
    REQUIRE(dynamic.valid());

    dynamic.flags = dynamic.flags | DebugDynamicFlags::DataPointersAreTagged;
    REQUIRE(dynamic.valid());
    dynamic.flags = dynamic.flags | DebugDynamicFlags::HasSlotState |
                    DebugDynamicFlags::SlotStateIsTaggedPointer;
    dynamic.state_offset = 2 * sizeof(std::size_t);
    REQUIRE(dynamic.valid());
    dynamic.flags = static_cast<DebugDynamicFlags>(
        static_cast<std::uint32_t>(dynamic.flags) &
        ~static_cast<std::uint32_t>(DebugDynamicFlags::DataPointersAreTagged));
    REQUIRE_FALSE(dynamic.valid());

    dynamic.flags = DebugDynamicFlags::DataIsIndirect | DebugDynamicFlags::DataIsPointerTable |
                    DebugDynamicFlags::HasSlotState | DebugDynamicFlags::SlotStateIsTaggedPointer;
    REQUIRE_FALSE(dynamic.valid());
    dynamic.flags = DebugDynamicFlags::DataIsIndirect | DebugDynamicFlags::DataIsPointerTable |
                    DebugDynamicFlags::DataPointersAreTagged | DebugDynamicFlags::SlotStateIsTaggedPointer;
    REQUIRE_FALSE(dynamic.valid());

    dynamic.flags = DebugDynamicFlags::DataIsIndirect | DebugDynamicFlags::DataIsPointerTable |
                    DebugDynamicFlags::ElementsAreOwners;
    dynamic.state_offset = 0;
    dynamic.flags = dynamic.flags | DebugDynamicFlags::ElementsArePointers;
    REQUIRE_FALSE(dynamic.valid());
}

TEST_CASE("type record common layouts are fixed", "[type-erasure][type-record]")
{
    using namespace hgraph;

    STATIC_REQUIRE(std::is_standard_layout_v<SchemaHeader>);
    STATIC_REQUIRE(std::is_trivially_copyable_v<SchemaHeader>);
    STATIC_REQUIRE(alignof(SchemaHeader) == alignof(void *));
    STATIC_REQUIRE(sizeof(SchemaHeader) == 8 + 2 * sizeof(void *));
    STATIC_REQUIRE(offsetof(SchemaHeader, magic) == 0);
    STATIC_REQUIRE(offsetof(SchemaHeader, abi_version) == 4);
    STATIC_REQUIRE(offsetof(SchemaHeader, family) == 6);
    STATIC_REQUIRE(offsetof(SchemaHeader, kind) == 7);
    STATIC_REQUIRE(offsetof(SchemaHeader, label) == 8);
    STATIC_REQUIRE(offsetof(SchemaHeader, introspection) == 8 + sizeof(void *));

    STATIC_REQUIRE(std::is_standard_layout_v<TypeClassification>);
    STATIC_REQUIRE(std::is_trivially_copyable_v<TypeClassification>);
    STATIC_REQUIRE(sizeof(TypeClassification) == 3);
    STATIC_REQUIRE(offsetof(TypeClassification, family) == 0);
    STATIC_REQUIRE(offsetof(TypeClassification, role) == 1);
    STATIC_REQUIRE(offsetof(TypeClassification, kind) == 2);

    STATIC_REQUIRE(std::is_standard_layout_v<TypeRecord>);
    STATIC_REQUIRE(std::is_trivially_copyable_v<TypeRecord>);
    STATIC_REQUIRE(alignof(TypeRecord) == alignof(void *));
    STATIC_REQUIRE(sizeof(TypeRecord) == 16 + 5 * sizeof(void *));
    STATIC_REQUIRE(offsetof(TypeRecord, magic) == 0);
    STATIC_REQUIRE(offsetof(TypeRecord, abi_version) == 4);
    STATIC_REQUIRE(offsetof(TypeRecord, role) == 6);
    STATIC_REQUIRE(offsetof(TypeRecord, reserved0) == 7);
    STATIC_REQUIRE(offsetof(TypeRecord, ops_abi_version) == 8);
    STATIC_REQUIRE(offsetof(TypeRecord, reserved1) == 10);
    STATIC_REQUIRE(offsetof(TypeRecord, capabilities) == 12);
    STATIC_REQUIRE(offsetof(TypeRecord, implementation_label) == 16);
    STATIC_REQUIRE(offsetof(TypeRecord, schema) == 16 + sizeof(void *));
    STATIC_REQUIRE(offsetof(TypeRecord, plan) == 16 + 2 * sizeof(void *));
    STATIC_REQUIRE(offsetof(TypeRecord, ops) == 16 + 3 * sizeof(void *));
    STATIC_REQUIRE(offsetof(TypeRecord, debug) == 16 + 4 * sizeof(void *));

    STATIC_REQUIRE(std::is_standard_layout_v<TypeRecordKey>);
    STATIC_REQUIRE(std::is_trivially_copyable_v<TypeRecordKey>);
    STATIC_REQUIRE(alignof(TypeRecordKey) == alignof(void *));
    STATIC_REQUIRE(sizeof(TypeRecordKey) == 5 * sizeof(void *));
    STATIC_REQUIRE(offsetof(TypeRecordKey, schema) == 0);
    STATIC_REQUIRE(offsetof(TypeRecordKey, role) == sizeof(void *));
    STATIC_REQUIRE(offsetof(TypeRecordKey, plan) == 2 * sizeof(void *));
    STATIC_REQUIRE(offsetof(TypeRecordKey, ops) == 3 * sizeof(void *));
    STATIC_REQUIRE(offsetof(TypeRecordKey, debug) == 4 * sizeof(void *));

    STATIC_REQUIRE(std::is_standard_layout_v<MockSchema>);
    STATIC_REQUIRE(offsetof(MockSchema, header) == 0);
}

TEST_CASE("schema headers construct valid semantic metadata", "[type-erasure][type-record]")
{
    using namespace hgraph;

    constexpr SchemaHeader empty{};
    STATIC_REQUIRE_FALSE(empty.valid());

    constexpr SchemaHeader header{TypeFamily::Value, TYPE_KIND_NONE, "mock"};
    STATIC_REQUIRE(header.magic == SCHEMA_HEADER_MAGIC);
    STATIC_REQUIRE(header.abi_version == SCHEMA_HEADER_ABI_VERSION);
    STATIC_REQUIRE(header.family == TypeFamily::Value);
    STATIC_REQUIRE(header.kind == TYPE_KIND_NONE);
    STATIC_REQUIRE(header.label[0] == 'm');
    STATIC_REQUIRE(header.introspection == nullptr);
    STATIC_REQUIRE(header.valid());
}

TEST_CASE("schema header validation rejects every malformed common field", "[type-erasure][type-record]")
{
    using namespace hgraph;

    SchemaHeader header{TypeFamily::Value, 7, "mock"};
    REQUIRE(header.valid());

    SECTION("magic") { header.magic = 0; }
    SECTION("ABI") { header.abi_version = 0; }
    SECTION("invalid family") { header.family = TypeFamily::Invalid; }
    SECTION("unknown family") { header.family = static_cast<TypeFamily>(0xff); }
    SECTION("null label") { header.label = nullptr; }
    SECTION("empty label") { header.label = ""; }

    REQUIRE_FALSE(header.valid());
}

TEST_CASE("type record registry accepts every approved family and role pair", "[type-erasure][type-record]")
{
    using namespace hgraph;
    static constexpr std::array families{TypeFamily::Value, TypeFamily::TimeSeries, TypeFamily::Node,
                                         TypeFamily::Graph, TypeFamily::Executor,   TypeFamily::Clock};
    MockOps ops{1};

    for (TypeFamily family : families)
    {
        const SchemaHeader schema{family, TYPE_KIND_NONE, "mock"};
        const TypeRecord &record =
            TypeRecordRegistry::instance().intern(definition_for(schema, allowed_role(family), &ops));
        REQUIRE(record.valid());
        REQUIRE(record.schema == &schema);
        REQUIRE(record.role == allowed_role(family));
    }
}

TEST_CASE("type record registry rejects every unapproved family and role pair", "[type-erasure][type-record]")
{
    using namespace hgraph;
    static constexpr std::array families{TypeFamily::Value, TypeFamily::TimeSeries, TypeFamily::Node,
                                         TypeFamily::Graph, TypeFamily::Executor,   TypeFamily::Clock};
    static constexpr std::array roles{TypeRole::Invalid, TypeRole::Instance, TypeRole::Data,
                                      TypeRole::Runtime, TypeRole::Input, TypeRole::Output};
    MockOps ops{1};

    for (TypeFamily family : families)
    {
        for (TypeRole role : roles)
        {
            const bool allowed = family == TypeFamily::TimeSeries
                                     ? (role == TypeRole::Data || role == TypeRole::Input || role == TypeRole::Output)
                                     : role == allowed_role(family);
            if (allowed)
            {
                continue;
            }
            const SchemaHeader schema{family, TYPE_KIND_NONE, "mock"};
            REQUIRE_THROWS_AS(TypeRecordRegistry::instance().intern(definition_for(schema, role, &ops)),
                              std::invalid_argument);
        }
    }
}

TEST_CASE("type record registry rejects malformed definitions", "[type-erasure][type-record]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, TYPE_KIND_NONE, "mock"};
    MockOps ops{1};
    auto definition = definition_for(schema, TypeRole::Instance, &ops);
    MemoryUtils::StoragePlan invalid_plan{};

    SECTION("null schema") { definition.key.schema = nullptr; }
    SECTION("bad schema magic") { schema.magic = 0; }
    SECTION("bad schema ABI") { schema.abi_version = 0; }
    SECTION("invalid schema family") { schema.family = TypeFamily::Invalid; }
    SECTION("unknown schema family") { schema.family = static_cast<TypeFamily>(0xff); }
    SECTION("null semantic label") { schema.label = nullptr; }
    SECTION("empty semantic label") { schema.label = ""; }
    SECTION("invalid role") { definition.key.role = TypeRole::Invalid; }
    SECTION("unknown role") { definition.key.role = static_cast<TypeRole>(0xff); }
    SECTION("disallowed role") { definition.key.role = TypeRole::Runtime; }
    SECTION("zero ops ABI") { definition.ops_abi_version = INVALID_OPS_ABI_VERSION; }
    SECTION("unknown capability") { definition.capabilities = static_cast<TypeCapabilities>(1u << 31u); }
    SECTION("null plan") { definition.key.plan = nullptr; }
    SECTION("invalid plan") { definition.key.plan = &invalid_plan; }
    SECTION("null ops") { definition.key.ops = nullptr; }

    REQUIRE_THROWS_AS(TypeRecordRegistry::instance().intern(definition), std::invalid_argument);
}

TEST_CASE("type record interning is canonical by key and label content", "[type-erasure][type-record]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 2, "mock"};
    MockOps ops{1};
    std::string first_label{"native"};
    std::string second_label{"native"};

    const TypeRecord &first = TypeRecordRegistry::instance().intern(
        definition_for(schema, TypeRole::Instance, &ops, first_label, TypeCapabilities::Viewable));
    const TypeRecord &second = TypeRecordRegistry::instance().intern(
        definition_for(schema, TypeRole::Instance, &ops, second_label, TypeCapabilities::Viewable));
    REQUIRE(&first == &second);
    REQUIRE(first.implementation_label != first_label.c_str());
    REQUIRE(static_cast<bool>(first.implementation_name() == "native"));
}

TEST_CASE("every type record key component participates in identity", "[type-erasure][type-record]")
{
    using namespace hgraph;
    SchemaHeader schema_a{TypeFamily::Value, 1, "a"};
    SchemaHeader schema_b{TypeFamily::Value, 1, "b"};
    MockOps ops_a{1};
    MockOps ops_b{2};
    int debug_a{};
    int debug_b{};
    const auto *plan_a = &MemoryUtils::plan_for<std::uint32_t>();
    const auto *plan_b = &MemoryUtils::plan_for<std::uint64_t>();

    const TypeRecordKey base{&schema_a, TypeRole::Instance, plan_a, &ops_a, debug_address(&debug_a)};
    REQUIRE(base != TypeRecordKey{&schema_b, TypeRole::Instance, plan_a, &ops_a, debug_address(&debug_a)});
    REQUIRE(base != TypeRecordKey{&schema_a, TypeRole::Data, plan_a, &ops_a, debug_address(&debug_a)});
    REQUIRE(base != TypeRecordKey{&schema_a, TypeRole::Instance, plan_b, &ops_a, debug_address(&debug_a)});
    REQUIRE(base != TypeRecordKey{&schema_a, TypeRole::Instance, plan_a, &ops_b, debug_address(&debug_a)});
    REQUIRE(base != TypeRecordKey{&schema_a, TypeRole::Instance, plan_a, &ops_a, debug_address(&debug_b)});

    auto &registry = TypeRecordRegistry::instance();
    const TypeRecord *schema_record = &registry.intern(definition_for(schema_a, TypeRole::Instance, &ops_a));
    const TypeRecord *other_schema = &registry.intern(definition_for(schema_b, TypeRole::Instance, &ops_a));
    const TypeRecord *plan_record =
        &registry.intern(definition_for(schema_a, TypeRole::Instance, &ops_a, {}, TypeCapabilities::None, plan_b));
    const TypeRecord *ops_record = &registry.intern(definition_for(schema_a, TypeRole::Instance, &ops_b));
    const TypeRecord *debug_record = &registry.intern(definition_for(
        schema_a, TypeRole::Instance, &ops_a, {}, TypeCapabilities::None, plan_a, debug_address(&debug_a)));
    REQUIRE(schema_record != other_schema);
    REQUIRE(schema_record != plan_record);
    REQUIRE(schema_record != ops_record);
    REQUIRE(schema_record != debug_record);
}

TEST_CASE("conflicting metadata preserves the canonical type record", "[type-erasure][type-record]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "mock"};
    MockOps ops{1};
    auto original = definition_for(schema, TypeRole::Instance, &ops, "native", TypeCapabilities::Viewable);
    auto &registry = TypeRecordRegistry::instance();
    const TypeRecord *record = &registry.intern(original);

    SECTION("ops ABI")
    {
        auto conflict = original;
        conflict.ops_abi_version = 2;
        REQUIRE_THROWS_AS(registry.intern(conflict), std::logic_error);
    }
    SECTION("capabilities")
    {
        auto conflict = original;
        conflict.capabilities = TypeCapabilities::Mutable;
        REQUIRE_THROWS_AS(registry.intern(conflict), std::logic_error);
    }
    SECTION("implementation label")
    {
        auto conflict = original;
        conflict.implementation_label = "python";
        REQUIRE_THROWS_AS(registry.intern(conflict), std::logic_error);
    }

    REQUIRE(registry.find(original.key) == record);
    REQUIRE(record->ops_abi_version == 1);
    REQUIRE(record->capabilities == TypeCapabilities::Viewable);
    REQUIRE(static_cast<bool>(record->implementation_name() == "native"));
}

TEST_CASE("type record owns implementation labels but borrows schema labels", "[type-erasure][type-record]")
{
    using namespace hgraph;
    std::string semantic_label{"semantic"};
    SchemaHeader schema{TypeFamily::Value, TYPE_KIND_NONE, semantic_label.c_str()};
    MockOps ops{1};
    const TypeRecord *record{};

    {
        std::string temporary_label{"temporary implementation"};
        record =
            &TypeRecordRegistry::instance().intern(definition_for(schema, TypeRole::Instance, &ops, temporary_label));
        REQUIRE(record->implementation_label != temporary_label.c_str());
        temporary_label.assign("mutated source label");
        REQUIRE(static_cast<bool>(record->implementation_name() == "temporary implementation"));
    }

    REQUIRE(static_cast<bool>(record->implementation_name() == "temporary implementation"));
    REQUIRE(record->schema->label == semantic_label.c_str());
    REQUIRE(static_cast<bool>(record->semantic_name() == "semantic"));
}

TEST_CASE("type record classification and diagnostic names are derived without "
          "duplication",
          "[type-erasure][type-record]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Node, 42, "semantic node"};
    MockOps ops{1};
    auto &registry = TypeRecordRegistry::instance();

    const TypeRecord &semantic = registry.intern(definition_for(schema, TypeRole::Runtime, &ops));
    REQUIRE(semantic.classification().family == TypeFamily::Node);
    REQUIRE(semantic.classification().role == TypeRole::Runtime);
    REQUIRE(semantic.classification().kind == 42);
    REQUIRE(static_cast<bool>(semantic.semantic_name() == "semantic node"));
    REQUIRE(semantic.implementation_name().empty());
    REQUIRE(static_cast<bool>(semantic.effective_name() == "semantic node"));

    MockOps other_ops{2};
    const TypeRecord &implemented =
        registry.intern(definition_for(schema, TypeRole::Runtime, &other_ops, "native compute"));
    REQUIRE(static_cast<bool>(implemented.effective_name() == "native compute"));
}

TEST_CASE("copied type record validation rejects every corrupt common field", "[type-erasure][type-record]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, TYPE_KIND_NONE, "mock"};
    MockOps ops{1};
    const TypeRecord &canonical = TypeRecordRegistry::instance().intern(
        definition_for(schema, TypeRole::Instance, &ops, "native", TypeCapabilities::Viewable));
    TypeRecord copy = canonical;
    SchemaHeader bad_schema = schema;
    MemoryUtils::StoragePlan invalid_plan{};

    SECTION("record magic") { copy.magic = 0; }
    SECTION("record ABI") { copy.abi_version = 0; }
    SECTION("reserved byte") { copy.reserved0 = 1; }
    SECTION("reserved word") { copy.reserved1 = 1; }
    SECTION("null schema") { copy.schema = nullptr; }
    SECTION("schema magic")
    {
        bad_schema.magic = 0;
        copy.schema = &bad_schema;
    }
    SECTION("schema ABI")
    {
        bad_schema.abi_version = 0;
        copy.schema = &bad_schema;
    }
    SECTION("schema family")
    {
        bad_schema.family = TypeFamily::Invalid;
        copy.schema = &bad_schema;
    }
    SECTION("schema label")
    {
        bad_schema.label = "";
        copy.schema = &bad_schema;
    }
    SECTION("role") { copy.role = TypeRole::Runtime; }
    SECTION("null plan") { copy.plan = nullptr; }
    SECTION("invalid plan") { copy.plan = &invalid_plan; }
    SECTION("ops ABI") { copy.ops_abi_version = INVALID_OPS_ABI_VERSION; }
    SECTION("capabilities") { copy.capabilities = static_cast<TypeCapabilities>(1u << 31u); }
    SECTION("ops") { copy.ops = nullptr; }
    SECTION("empty implementation label") { copy.implementation_label = ""; }

    REQUIRE_FALSE(copy.valid());
    REQUIRE(canonical.valid());
}

TEST_CASE("type record find and reset policy is explicit", "[type-erasure][type-record]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "mock"};
    MockOps ops{1};
    auto definition = definition_for(schema, TypeRole::Instance, &ops);
    auto &registry = TypeRecordRegistry::instance();

    REQUIRE(registry.find(definition.key) == nullptr);
    const TypeRecord &record = registry.intern(definition);
    REQUIRE(registry.find(definition.key) == &record);
    registry.reset();
    REQUIRE(registry.find(definition.key) == nullptr);
}

TEST_CASE("concurrent type record interning returns one canonical address", "[type-erasure][type-record]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "mock"};
    MockOps ops{1};
    auto definition = definition_for(schema, TypeRole::Instance, &ops, "native");
    constexpr std::size_t thread_count = 8;
    constexpr std::size_t intern_count = 256;
    std::array<const TypeRecord *, thread_count> records{};
    std::barrier start{static_cast<std::ptrdiff_t>(thread_count)};
    std::array<std::thread, thread_count> threads;
    std::atomic_bool every_result_matched{true};

    for (std::size_t i = 0; i < thread_count; ++i)
    {
        threads[i] = std::thread(
            [&, i]
            {
                start.arrive_and_wait();
                const TypeRecord *thread_record = nullptr;
                for (std::size_t attempt = 0; attempt < intern_count; ++attempt)
                {
                    const TypeRecord *result = &TypeRecordRegistry::instance().intern(definition);
                    if (thread_record == nullptr)
                    {
                        thread_record = result;
                    }
                    else if (result != thread_record)
                    {
                        every_result_matched.store(false, std::memory_order_relaxed);
                    }
                }
                records[i] = thread_record;
            });
    }
    for (auto &thread : threads)
    {
        thread.join();
    }

    REQUIRE(every_result_matched.load(std::memory_order_relaxed));
    for (const TypeRecord *record : records)
    {
        REQUIRE(record == records.front());
    }
}

TEST_CASE("canonical registry reset clears type records before their lenders", "[type-erasure][type-record]")
{
    using namespace hgraph;
    SchemaHeader schema{TypeFamily::Value, 1, "mock"};
    MockOps ops{1};
    auto definition = definition_for(schema, TypeRole::Instance, &ops, "native");
    auto &registry = TypeRecordRegistry::instance();

    (void)registry.intern(definition);
    REQUIRE(registry.find(definition.key) != nullptr);
    reset_all_registries();
    REQUIRE(registry.find(definition.key) == nullptr);

    const TypeRecord &reinterned = registry.intern(definition);
    REQUIRE(reinterned.valid());
    REQUIRE(registry.find(definition.key) == &reinterned);
}

TEST_CASE("registry reset destroys converter-built operator defaults before their type records",
          "[type-erasure][type-record][reset]")
{
    using namespace hgraph;

    auto &types = TypeRegistry::instance();
    const auto *str = types.register_scalar<std::string>("str");
    const auto *list = types.list(str, 0);
    const JsonConverter &converter = json_converter(list);

    OperatorImpl impl;
    impl.name = "type_record_reset_nontrivial_default";
    ParamPattern parameter;
    parameter.kind = ParamPattern::Kind::Scalar;
    parameter.name = "items";
    parameter.default_value = from_json_string(converter, R"(["a deliberately non-small string payload"])");
    const TypeRecord *record = parameter.default_value->binding().record();
    REQUIRE(record != nullptr);
    REQUIRE(record->valid());
    impl.params.push_back(std::move(parameter));
    OperatorRegistry::instance().register_overload(std::move(impl));

    reset_all_registries();

    const auto reseeded_str = TypeRegistry::instance().scalar_type<std::string>();
    REQUIRE(reseeded_str);
    Value after_reset{std::string{"records and converter caches were rebuilt"}};
    REQUIRE(after_reset.binding() == reseeded_str);
    REQUIRE(after_reset.as<std::string>() == "records and converter caches were rebuilt");
}
