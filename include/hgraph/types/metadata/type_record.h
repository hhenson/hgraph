#ifndef HGRAPH_TYPES_METADATA_TYPE_RECORD_H
#define HGRAPH_TYPES_METADATA_TYPE_RECORD_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/utils/memory_utils.h>

#include <cstdint>
#include <string_view>

namespace hgraph
{
    struct SchemaIntrospection;
    struct DebugDescriptor;

    enum class TypeFamily : std::uint8_t
    {
        Invalid = 0,
        Value = 1,
        TimeSeries = 2,
        Node = 3,
        Graph = 4,
        Executor = 5,
        Clock = 6,
    };

    enum class TypeRole : std::uint8_t
    {
        Invalid = 0,
        Instance = 1,
        Data = 2,
        Runtime = 3,
        Input = 4,
        Output = 5,
    };

    using TypeKind = std::uint8_t;
    inline constexpr TypeKind TYPE_KIND_NONE = 0xff;

    enum class TypeCapabilities : std::uint32_t
    {
        None = 0,
        Constructible = 1u << 0u,
        Destructible = 1u << 1u,
        Copyable = 1u << 2u,
        Movable = 1u << 3u,
        Mutable = 1u << 4u,
        Equatable = 1u << 5u,
        Comparable = 1u << 6u,
        Hashable = 1u << 7u,
        HasChildren = 1u << 8u,
        Viewable = 1u << 9u,
    };

    [[nodiscard]] constexpr TypeCapabilities operator|(TypeCapabilities lhs, TypeCapabilities rhs) noexcept
    {
        return static_cast<TypeCapabilities>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
    }

    [[nodiscard]] constexpr TypeCapabilities operator&(TypeCapabilities lhs, TypeCapabilities rhs) noexcept
    {
        return static_cast<TypeCapabilities>(static_cast<std::uint32_t>(lhs) & static_cast<std::uint32_t>(rhs));
    }

    constexpr TypeCapabilities &operator|=(TypeCapabilities &lhs, TypeCapabilities rhs) noexcept
    {
        lhs = lhs | rhs;
        return lhs;
    }

    [[nodiscard]] constexpr bool has_capability(TypeCapabilities capabilities, TypeCapabilities requested) noexcept
    {
        return (capabilities & requested) == requested;
    }

    inline constexpr std::uint32_t SCHEMA_HEADER_MAGIC = 0x48475348u;
    inline constexpr std::uint32_t TYPE_RECORD_MAGIC = 0x48475452u;
    inline constexpr std::uint16_t SCHEMA_HEADER_ABI_VERSION = 1;
    inline constexpr std::uint16_t TYPE_RECORD_ABI_VERSION = 2;
    inline constexpr std::uint16_t INVALID_OPS_ABI_VERSION = 0;

    struct SchemaHeader
    {
        std::uint32_t magic{0};
        std::uint16_t abi_version{0};
        TypeFamily family{TypeFamily::Invalid};
        TypeKind kind{TYPE_KIND_NONE};
        const char *label{nullptr};
        const SchemaIntrospection *introspection{nullptr};

        constexpr SchemaHeader() noexcept = default;

        constexpr SchemaHeader(TypeFamily schema_family, TypeKind schema_kind, const char *schema_label,
                               const SchemaIntrospection *schema_introspection = nullptr) noexcept
            : magic(SCHEMA_HEADER_MAGIC), abi_version(SCHEMA_HEADER_ABI_VERSION), family(schema_family),
              kind(schema_kind), label(schema_label), introspection(schema_introspection)
        {
        }

        [[nodiscard]] constexpr bool valid() const noexcept
        {
            return magic == SCHEMA_HEADER_MAGIC && abi_version == SCHEMA_HEADER_ABI_VERSION &&
                   family >= TypeFamily::Value && family <= TypeFamily::Clock && label != nullptr && label[0] != '\0';
        }
    };

    struct TypeClassification
    {
        TypeFamily family{TypeFamily::Invalid};
        TypeRole role{TypeRole::Invalid};
        TypeKind kind{TYPE_KIND_NONE};
    };

    class TypeRecordRegistry;

    struct HGRAPH_EXPORT TypeRecord
    {
        std::uint32_t magic;
        std::uint16_t abi_version;
        TypeRole role;
        std::uint8_t reserved0;
        std::uint16_t ops_abi_version;
        std::uint16_t reserved1;
        TypeCapabilities capabilities;
        const char *implementation_label;
        const SchemaHeader *schema;
        const MemoryUtils::StoragePlan *plan;
        const void *ops;
        const DebugDescriptor *debug;
        /** Ordinary owning representation for graph-local value records.
            Null means that the record's normal ValueOps owning projection is
            authoritative.  Published once before the record is exposed to a
            running graph. */
        mutable const TypeRecord *external_value_owner;

        [[nodiscard]] bool valid() const noexcept;

        [[nodiscard]] constexpr TypeClassification classification() const noexcept
        {
            return schema == nullptr ? TypeClassification{TypeFamily::Invalid, role, TYPE_KIND_NONE}
                                     : TypeClassification{schema->family, role, schema->kind};
        }

        [[nodiscard]] constexpr std::string_view semantic_name() const noexcept
        {
            return schema == nullptr || schema->label == nullptr ? std::string_view{} : std::string_view{schema->label};
        }

        [[nodiscard]] constexpr std::string_view implementation_name() const noexcept
        {
            return implementation_label == nullptr ? std::string_view{} : std::string_view{implementation_label};
        }

        [[nodiscard]] constexpr std::string_view effective_name() const noexcept
        {
            return implementation_name().empty() ? semantic_name() : implementation_name();
        }

    private:
        friend class TypeRecordRegistry;

        constexpr TypeRecord(TypeRole record_role, std::uint16_t record_ops_abi_version,
                             TypeCapabilities record_capabilities, const char *record_implementation_label,
                             const SchemaHeader *record_schema, const MemoryUtils::StoragePlan *record_plan,
                             const void *record_ops, const DebugDescriptor *record_debug) noexcept
            : magic(TYPE_RECORD_MAGIC), abi_version(TYPE_RECORD_ABI_VERSION), role(record_role), reserved0(0),
              ops_abi_version(record_ops_abi_version), reserved1(0), capabilities(record_capabilities),
              implementation_label(record_implementation_label), schema(record_schema), plan(record_plan),
              ops(record_ops), debug(record_debug), external_value_owner(nullptr)
        {
        }
    };
} // namespace hgraph

#endif // HGRAPH_TYPES_METADATA_TYPE_RECORD_H
